use std::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use crc::{Crc, CRC_16_IBM_SDLC};
use crate::hash_table::FileLocation;

/// Trait for hash table operations needed during merge
pub trait HashTableTrait {
    fn delete(&mut self, key: &str) -> bool;
    fn insert(&mut self, key: &str, location: FileLocation);
}

/// Tombstone marker used to indicate deleted keys
/// Using a special sequence that's unlikely to appear in normal data
pub const TOMBSTONE_MARKER: &str = "\\DELETED\\";

/// Custom error type for storage operations
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    KeyDeleted(String),
    CorruptedData(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "IO error: {}", e),
            StorageError::KeyDeleted(key) => write!(f, "Key '{}' has been deleted", key),
            StorageError::CorruptedData(msg) => write!(f, "Data corruption: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        StorageError::Io(error)
    }
}

/// File-based storage for key-value pairs with append-only semantics and file rotation
/// Stores entries in format: [key_size:4][value_size:4][key][value]
/// Creates new files when current file exceeds configurable size
pub struct Storage {
    storage_dir: PathBuf,
    current_file: File,
    current_filename: String,
    current_file_size: u64,
    file_counter: u32,
    max_file_size: u64,
}

impl Storage {
    /// Creates a new storage instance with storage directory and default file size (512 bytes)
    /// Files are named data_000.dat, data_001.dat, etc.
    pub fn new<P: AsRef<Path>>(storage_dir: P) -> std::io::Result<Storage> {
        Self::new_with_config(storage_dir, 512)
    }
    
    /// Creates a new storage instance with configurable directory and file size
    /// Files are named data_000.dat, data_001.dat, etc.
    pub fn new_with_config<P: AsRef<Path>>(storage_dir: P, max_file_size: u64) -> std::io::Result<Storage> {
        let storage_dir = storage_dir.as_ref().to_path_buf();
        
        // Create storage directory if it doesn't exist
        create_dir_all(&storage_dir)?;
        
        // Start with first file
        let file_counter = 0;
        let current_filename = format!("data_{:03}.dat", file_counter);
        let file_path = storage_dir.join(&current_filename);
        
        let current_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&file_path)?;
            
        // Get current file size
        let current_file_size = current_file.metadata()?.len();
            
        Ok(Storage { 
            storage_dir,
            current_file,
            current_filename,
            current_file_size,
            file_counter,
            max_file_size,
        })
    }

    /// Writes a key-value pair to storage and returns the FileLocation
    /// Format: [key_size: 4 bytes][value_size: 4 bytes][key: key_size bytes][value: value_size bytes]
    /// Rotates to new file if current file would exceed 512 bytes
    /// filename, value_offset, value_size, crc
    pub fn write(&mut self, key: &str, value: &str) -> std::io::Result<(String, u64, u32, u16)> {
        // Calculate size of entry to be written
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();
        let entry_size = 8 + key_bytes.len() + value_bytes.len(); // 4 + 4 + key + value
        
        // Check if we need to rotate to a new file
        if self.current_file_size + entry_size as u64 > self.max_file_size {
            self.rotate_file()?;
        }
        
        // Get current file position (this will be our record start offset)
        let record_start = self.current_file.seek(SeekFrom::End(0))?;
        
        // Prepare data to write
        let key_size = key_bytes.len() as u32;
        let value_size = value_bytes.len() as u32;
        
        // Write in order: key_size, value_size, key, value
        self.current_file.write_all(&key_size.to_le_bytes())?;
        self.current_file.write_all(&value_size.to_le_bytes())?;
        self.current_file.write_all(key_bytes)?;
        self.current_file.write_all(value_bytes)?;
        self.current_file.flush()?;
        
        // Update current file size
        self.current_file_size += entry_size as u64;

        // Calculate value offset: record_start + key_size + value_size + key_bytes
        let value_offset = record_start + 4 + 4 + key_bytes.len() as u64;

        const X25: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);
        Ok((self.current_filename.clone(), value_offset, value_size, X25.checksum(value_bytes)))
    }

    /// Marks a key as deleted by writing a tombstone entry
    /// Returns the FileLocation of the tombstone
    pub fn delete(&mut self, key: &str) -> std::io::Result<(String, u64, u32, u16)> {
        self.write(key, TOMBSTONE_MARKER)
    }

    /// Rotates to a new storage file
    fn rotate_file(&mut self) -> std::io::Result<()> {
        self.file_counter += 1;
        self.current_filename = format!("data_{:03}.dat", self.file_counter);
        let file_path = self.storage_dir.join(&self.current_filename);
        
        self.current_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&file_path)?;
            
        self.current_file_size = 0;
        Ok(())
    }

    /// Reads a key-value pair from the specified file at the given byte offset
    /// Returns (key, value) if successful, or error if key is deleted
    pub fn read(&mut self, filename: &str, offset: u64) -> Result<(String, String), StorageError> {
        let file_path = self.storage_dir.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&file_path)?;
        
        // Seek to the offset
        file.seek(SeekFrom::Start(offset))?;
        
        // Read key_size and value_size (4 bytes each)
        let mut size_buf = [0u8; 4];
        file.read_exact(&mut size_buf)?;
        let key_size = u32::from_le_bytes(size_buf) as usize;
        
        file.read_exact(&mut size_buf)?;
        let value_size = u32::from_le_bytes(size_buf) as usize;
        
        // Read key
        let mut key_buf = vec![0u8; key_size];
        file.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        // Read value
        let mut value_buf = vec![0u8; value_size];
        file.read_exact(&mut value_buf)?;
        let value = String::from_utf8(value_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        // Check if this is a tombstone (deleted key)
        if value == TOMBSTONE_MARKER {
            return Err(StorageError::KeyDeleted(key));
        }
        
        Ok((key, value))
    }

    /// Reads only the value from the specified file at the given byte offset
    /// More efficient when key is not needed. Returns error if key is deleted or data is corrupted.
    pub fn read_value(&mut self, filename: &str, value_offset: u64, value_size: u32, expected_crc: u16, key: &str) -> Result<String, StorageError> {
        let file_path = self.storage_dir.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&file_path)?;
        
        // Seek to the offset
        file.seek(SeekFrom::Start(value_offset))?;

        let value_size = value_size as usize;

        // Read value
        let mut value_buf = vec![0u8; value_size];
        file.read_exact(&mut value_buf)?;
        
        // Verify CRC before converting to string
        const X25: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);
        let calculated_crc = X25.checksum(value_buf.as_slice());
        if calculated_crc != expected_crc {
            return Err(StorageError::CorruptedData(format!(
                "CRC mismatch for key '{}': expected {}, got {}", 
                key, expected_crc, calculated_crc
            )));
        }
        
        let value = String::from_utf8(value_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        // Check if this is a tombstone (deleted key)
        if value == TOMBSTONE_MARKER {
            return Err(StorageError::KeyDeleted(key.parse().unwrap()));
        }
        
        Ok(value)
    }
    
    /// Merges all inactive storage files, keeping only the latest value for each key
    /// Removes old entries and tombstones, compacting the storage into the current active file
    /// Also cleans up the hash table by removing entries for deleted keys
    /// This operation helps reclaim space and improve read performance
    pub fn merge_inactive_files<T>(&mut self, mut hash_table: Option<&mut T>) -> std::io::Result<()> 
    where 
        T: HashTableTrait,
    {
        // Collect all data files except the current active one
        let mut data_files = Vec::new();
        
        for entry in read_dir(&self.storage_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            
            if filename_str.starts_with("data_") && filename_str.ends_with(".dat") {
                if filename_str != self.current_filename {
                    data_files.push(filename_str.to_string());
                }
            }
        }
        
        if data_files.is_empty() {
            println!("  No inactive files to merge");
            return Ok(());
        }
        
        // Sort files to process them in order
        data_files.sort();
        
        // Read all entries from inactive files and track the latest value for each key
        let mut latest_entries: HashMap<String, String> = HashMap::new();
        let mut total_entries_read = 0;
        let mut tombstones_found = 0;
        
        for filename in &data_files {
            println!("  Processing inactive file: {}", filename);
            let file_path = self.storage_dir.join(filename);
            let mut file = File::open(&file_path)?;
            let mut position = 0u64;
            
            while position < file.metadata()?.len() {
                file.seek(SeekFrom::Start(position))?;
                
                // Read entry header
                let mut size_buf = [0u8; 4];
                if file.read_exact(&mut size_buf).is_err() {
                    break; // End of file or corrupted entry
                }
                let key_size = u32::from_le_bytes(size_buf) as usize;
                
                if file.read_exact(&mut size_buf).is_err() {
                    break; // End of file or corrupted entry
                }
                let value_size = u32::from_le_bytes(size_buf) as usize;
                
                // Read key
                let mut key_buf = vec![0u8; key_size];
                if file.read_exact(&mut key_buf).is_err() {
                    break;
                }
                let key = String::from_utf8(key_buf).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;
                
                // Read value
                let mut value_buf = vec![0u8; value_size];
                if file.read_exact(&mut value_buf).is_err() {
                    break;
                }
                let value = String::from_utf8(value_buf).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;
                
                total_entries_read += 1;
                
                // Track latest value (including tombstones)
                if value == TOMBSTONE_MARKER {
                    tombstones_found += 1;
                    latest_entries.insert(key, value); // Keep tombstone as latest
                } else {
                    latest_entries.insert(key, value);
                }
                
                // Move to next entry
                position += 8 + key_size as u64 + value_size as u64;
            }
        }
        
        println!("  Read {} total entries from {} inactive files", total_entries_read, data_files.len());
        println!("  Found {} unique keys ({} tombstones)", latest_entries.len(), tombstones_found);
        
        // Write non-deleted entries to current active file and update hash table
        let mut entries_written = 0;
        let mut tombstones_skipped = 0;
        let mut hash_table_deletions = 0;
        
        for (key, value) in latest_entries {
            if value == TOMBSTONE_MARKER {
                tombstones_skipped += 1;
                // Remove deleted key from hash table if provided
                if let Some(ref mut ht) = hash_table {
                    if ht.delete(&key) {
                        hash_table_deletions += 1;
                    }
                }
                // Skip tombstones - they represent deleted keys
                continue;
            }
            
            // Write the latest value to current active file
            // filename, value_offset, value_size, crc
            let (filename, value_offset, value_size, crc) = self.write(&key, &value)?;
            
            // Update hash table with new location if provided
            if let Some(ref mut ht) = hash_table {
                ht.insert(&key, FileLocation::new(filename, value_size, value_offset, crc));
            }
            
            entries_written += 1;
        }
        
        println!("  Wrote {} active entries to current file, skipped {} deleted entries", 
            entries_written, tombstones_skipped);
        
        if hash_table.is_some() {
            println!("  Removed {} deleted keys from hash table", hash_table_deletions);
        }
        
        // Remove the old inactive files
        for filename in &data_files {
            let file_path = self.storage_dir.join(filename);
            remove_file(file_path)?;
            println!("  Removed old file: {}", filename);
        }
        
        println!("  Merge completed successfully");
        Ok(())
    }
    
    /// Returns statistics about the storage files
    pub fn get_storage_stats(&self) -> std::io::Result<()> {
        let mut file_count = 0;
        let mut total_size = 0u64;
        
        for entry in read_dir(&self.storage_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            
            if filename_str.starts_with("data_") && filename_str.ends_with(".dat") {
                let metadata = entry.metadata()?;
                let size = metadata.len();
                total_size += size;
                file_count += 1;
                
                let status = if filename_str == self.current_filename { " (ACTIVE)" } else { "" };
                println!("    {}: {} bytes{}", filename_str, size, status);
            }
        }
        
        println!("  Total: {} files, {} bytes", file_count, total_size);
        Ok(())
    }
}