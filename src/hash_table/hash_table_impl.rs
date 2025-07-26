use std::time::{SystemTime, UNIX_EPOCH};
use crate::storage::HashTableTrait;

/// Represents a file location with filename and byte offset
#[derive(Debug, Clone)]
pub struct FileLocation {
    pub filename: String,
    pub value_size: u32,
    pub value_offset: u64,
    pub crc: u16,
    pub timestamp: u64,
}

impl FileLocation {
    pub fn new(filename: String, value_size: u32, value_offset: u64, crc: u16) -> Self {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let nanos = since_epoch.as_secs() * 1_000_000_000 + since_epoch.subsec_nanos() as u64;
        FileLocation { filename, value_size, value_offset, crc, timestamp: nanos }
    }
}

/// Represents a key-value pair in the hash table
/// Key is stored as String, value as FileLocation (filename + byte offset)
#[derive(Debug, Clone)]
pub struct Entry {
    pub key: String,
    pub value: FileLocation,
}

/// Defines the collision resolution strategy for the hash table
#[derive(Debug, Clone)]
pub enum CollisionResolution {
    /// Linear probing: check next slot sequentially (i+1, i+2, ...)
    LinearProbing,
    /// Quadratic probing: check slots with quadratic intervals (i+1², i+2², ...)
    QuadraticProbing,
    /// Double hashing: use second hash function for step size
    DoubleHashing,
    /// Chaining: store colliding entries in linked lists per bucket
    Chaining,
}

/// Hash table implementation supporting multiple collision resolution strategies
/// Stores byte offsets as values (u64)
#[derive(Debug, Clone)]
pub struct HashTable {
    /// Main storage array for open addressing methods (linear, quadratic, double hashing)
    buckets: Vec<Option<Entry>>,
    /// Separate chaining storage - vector of chains for each bucket
    chains: Vec<Vec<Entry>>,
    /// Number of buckets in the hash table
    size: u64,
    /// Which collision resolution method to use
    collision_method: CollisionResolution,
}

impl Entry {
    /// Creates a new entry with the given key and file location
    pub fn new(key: &str, value: FileLocation) -> Entry {
        Entry { key: key.to_string(), value }
    }
}

impl HashTable {
    /// Creates a new hash table with specified size and collision resolution method
    pub fn new(size: u64, collision_method: CollisionResolution) -> HashTable {
        let buckets = vec![None; size as usize];
        let chains = vec![Vec::new(); size as usize];
        HashTable { size, buckets, chains, collision_method }
    }

    /// Creates a hash table using linear probing for collision resolution
    pub fn new_linear_probing(size: u64) -> HashTable {
        Self::new(size, CollisionResolution::LinearProbing)
    }

    /// Creates a hash table using quadratic probing for collision resolution
    pub fn new_quadratic_probing(size: u64) -> HashTable {
        Self::new(size, CollisionResolution::QuadraticProbing)
    }

    /// Creates a hash table using double hashing for collision resolution
    pub fn new_double_hashing(size: u64) -> HashTable {
        Self::new(size, CollisionResolution::DoubleHashing)
    }

    /// Creates a hash table using separate chaining for collision resolution
    pub fn new_chaining(size: u64) -> HashTable {
        Self::new(size, CollisionResolution::Chaining)
    }

    /// Inserts a key with file location into the hash table
    /// Uses the configured collision resolution method
    pub fn insert(&mut self, key: &str, value: FileLocation) {
        match self.collision_method {
            CollisionResolution::Chaining => self.insert_chaining(key, value),
            _ => self.insert_open_addressing(key, value),
        }
    }

    /// Insert using separate chaining - each bucket contains a vector of entries
    fn insert_chaining(&mut self, key: &str, value: FileLocation) {
        let index = (get_hash(key) % self.size) as usize;
        let chain = &mut self.chains[index];
        
        // Check if key already exists in chain and update it
        for entry in chain.iter_mut() {
            if entry.key == key {
                entry.value = value;
                return;
            }
        }
        
        // Key doesn't exist, add new entry to the chain
        chain.push(Entry::new(key, value));
    }

    /// Insert using open addressing (linear, quadratic, or double hashing)
    fn insert_open_addressing(&mut self, key: &str, value: FileLocation) {
        let base_index = (get_hash(key) % self.size) as usize;
        let mut attempt = 0;
        
        loop {
            let index = self.get_probe_index(base_index, attempt, key);
            
            match &mut self.buckets[index] {
                None => {
                    // Found empty slot, insert here
                    self.buckets[index] = Some(Entry::new(key, value));
                    return;
                }
                Some(entry) => {
                    if entry.key == key {
                        // Key already exists, update value
                        entry.value = value;
                        return;
                    }
                    // Collision occurred, try next probe position
                    attempt += 1;
                    if attempt >= self.size {
                        panic!("Hash table is full");
                    }
                }
            }
        }
    }

    /// Calculates the next probe index based on collision resolution method
    fn get_probe_index(&self, base_index: usize, attempt: u64, key: &str) -> usize {
        match self.collision_method {
            CollisionResolution::LinearProbing => {
                // Linear probing: check next slot sequentially
                (base_index + attempt as usize) % (self.size as usize)
            }
            CollisionResolution::QuadraticProbing => {
                // Quadratic probing: use quadratic function for step size
                (base_index + (attempt * attempt) as usize) % (self.size as usize)
            }
            CollisionResolution::DoubleHashing => {
                // Double hashing: derive second hash from first hash
                let hash1 = get_hash(key);
                let hash2 = 7 - (hash1 % 7); // Ensures non-zero step size (1-7)
                (base_index + (attempt * hash2 as u64) as usize) % (self.size as usize)
            }
            CollisionResolution::Chaining => base_index, // Not used for chaining
        }
    }

    /// Removes a key-value pair from the hash table
    /// Returns true if the key was found and deleted, false otherwise
    pub fn delete(&mut self, key: &str) -> bool {
        match self.collision_method {
            CollisionResolution::Chaining => self.delete_chaining(key),
            _ => self.delete_open_addressing(key),
        }
    }

    /// Delete from separate chaining - remove from the appropriate chain
    fn delete_chaining(&mut self, key: &str) -> bool {
        let index = (get_hash(key) % self.size) as usize;
        let chain = &mut self.chains[index];
        
        // Search through the chain for the key
        for i in 0..chain.len() {
            if chain[i].key == key {
                chain.remove(i);
                return true;
            }
        }
        false // Key not found
    }

    /// Delete from open addressing - requires rehashing to maintain probe sequences
    fn delete_open_addressing(&mut self, key: &str) -> bool {
        let base_index = (get_hash(key) % self.size) as usize;
        let mut attempt = 0;
        
        loop {
            let index = self.get_probe_index(base_index, attempt, key);
            
            match &self.buckets[index] {
                None => return false, // Key not found (hit empty slot)
                Some(entry) => {
                    if entry.key == key {
                        // Found the key, delete it
                        self.buckets[index] = None;
                        // Rehash entries that might be affected by this deletion
                        self.rehash_cluster_generic(index);
                        return true;
                    }
                    attempt += 1;
                    if attempt >= self.size {
                        return false; // Searched entire table
                    }
                }
            }
        }
    }

    /// Retrieves the file location for a given key
    /// Returns Some(file_location) if found, None if key doesn't exist
    pub fn get(&self, key: &str) -> Option<&FileLocation> {
        match self.collision_method {
            CollisionResolution::Chaining => self.get_chaining(key),
            _ => self.get_open_addressing(key),
        }
    }

    /// Get from separate chaining - search through the appropriate chain
    fn get_chaining(&self, key: &str) -> Option<&FileLocation> {
        let index = (get_hash(key) % self.size) as usize;
        let chain = &self.chains[index];
        
        // Linear search through the chain
        for entry in chain {
            if entry.key == key {
                return Some(&entry.value);
            }
        }
        None // Key not found in chain
    }

    /// Get from open addressing - follow probe sequence until found or empty slot
    fn get_open_addressing(&self, key: &str) -> Option<&FileLocation> {
        let base_index = (get_hash(key) % self.size) as usize;
        let mut attempt = 0;
        
        loop {
            let index = self.get_probe_index(base_index, attempt, key);
            
            match &self.buckets[index] {
                None => return None, // Hit empty slot, key not found
                Some(entry) => {
                    if entry.key == key {
                        return Some(&entry.value); // Found the key
                    }
                    // Continue probing
                    attempt += 1;
                    if attempt >= self.size {
                        return None; // Searched entire table
                    }
                }
            }
        }
    }

    /// Rehashes entries after deletion to maintain probe sequence integrity
    fn rehash_cluster_generic(&mut self, deleted_index: usize) {
        match self.collision_method {
            CollisionResolution::LinearProbing => self.rehash_cluster_linear(deleted_index),
            _ => self.rehash_cluster_general(deleted_index),
        }
    }

    /// Optimized rehashing for linear probing - only rehash affected cluster
    fn rehash_cluster_linear(&mut self, deleted_index: usize) {
        let mut index = (deleted_index + 1) % (self.size as usize);
        
        // Continue until we hit an empty slot (end of cluster)
        while let Some(entry) = self.buckets[index].take() {
            let original_index = (get_hash(&entry.key) % self.size) as usize;
            
            // Check if this entry should be moved to fill the gap
            if self.should_move_entry(original_index, deleted_index, index) {
                self.insert(&entry.key, entry.value);
            } else {
                // Entry stays in current position
                self.buckets[index] = Some(entry);
            }
            
            index = (index + 1) % (self.size as usize);
        }
    }

    /// General rehashing for quadratic and double hashing - rehash entire table
    /// This is simpler but less efficient than cluster-specific rehashing
    fn rehash_cluster_general(&mut self, _deleted_index: usize) {
        let mut entries_to_reinsert = Vec::new();
        
        // Extract all entries from the table
        for i in 0..self.size as usize {
            if let Some(entry) = self.buckets[i].take() {
                entries_to_reinsert.push(entry);
            }
        }
        
        // Reinsert all entries (they'll find their correct positions)
        for entry in entries_to_reinsert {
            self.insert(&entry.key, entry.value);
        }
    }

    /// Determines if an entry should be moved to fill a deleted slot in linear probing
    /// This handles the wraparound case correctly
    fn should_move_entry(&self, original_index: usize, deleted_index: usize, current_index: usize) -> bool {
        if original_index <= deleted_index {
            // Normal case: check if current position is between deleted and original
            current_index > deleted_index || current_index < original_index
        } else {
            // Wraparound case: original index is after deleted index (wrapped around)
            current_index > deleted_index && current_index < original_index
        }
    }
}

/// Primary hash function using polynomial rolling hash with multiplier 31
/// This is a simple but effective hash function for strings
fn get_hash(key: &str) -> u64 {
    let mut hash = 0u64;
    for byte in key.bytes() {
        // Polynomial rolling hash: hash = hash * 31 + byte
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }
    hash
}

/// Implementation of HashTableTrait for merge operations
impl HashTableTrait for HashTable {
    fn delete(&mut self, key: &str) -> bool {
        self.delete(key)
    }
    
    fn insert(&mut self, key: &str, location: FileLocation) {
        self.insert(key, location);
    }
}


