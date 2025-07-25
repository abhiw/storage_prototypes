use std::io::{self, BufRead, BufReader, Write};
use std::time::{Duration, Instant};
use mio::{Events, Interest, Poll, Token};
use mio::unix::SourceFd;
use std::os::unix::io::AsRawFd;
use crate::{Storage, HashTable};
use crate::event_loop::EventLoop;

pub struct TerminalEventLoop;

const STDIN_TOKEN: Token = Token(0);

impl EventLoop for TerminalEventLoop {
    fn run(&mut self, storage: &mut Storage, hash_table: &mut HashTable, merge_interval_seconds: u64) {
        let mut poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(128);
        
        let fd = io::stdin().as_raw_fd();
        let mut stdin_fd = SourceFd(&fd);
        poll.registry().register(&mut stdin_fd, STDIN_TOKEN, Interest::READABLE).unwrap();

        let mut reader = BufReader::new(io::stdin());
        let mut buffer = String::new();

        let mut last_activity = Instant::now();
        let merge_timeout = Duration::from_secs(merge_interval_seconds);
        let mut operation_count = 0;

        loop {
            // Use a short poll timeout to regularly check for auto-merge
            // println!("[DEBUG] Polling for events...");
            match poll.poll(&mut events, Some(Duration::from_secs(1))) {
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue, // Retry on interrupt
                Err(e) => {
                    println!("Error polling for events: {}", e);
                    break;
                }
            }

            for event in events.iter() {
                if event.token() == STDIN_TOKEN {
                    // println!("[DEBUG] STDIN event received.");
                    // Read all available lines without blocking
                    loop {
                        match reader.read_line(&mut buffer) {
                            Ok(0) => { // EOF
                                println!("\nInput stream closed. Exiting.");
                                return;
                            }
                            Ok(_) => {
                                let input = buffer.trim();
                                if !input.is_empty() {
                                    print!("> ");
                                    io::stdout().flush().unwrap();
                                    println!("{}", input);

                                    last_activity = Instant::now();
                                    // println!("[DEBUG] Handling command: {}", input);
                                    if handle_command(input, storage, hash_table, &mut operation_count, merge_interval_seconds) {
                                        return; // Exit command was received
                                    }
                                }
                                buffer.clear();
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // println!("[DEBUG] read_line WouldBlock. No more lines for now.");
                                // No more lines to read for now
                                break;
                            }
                            Err(e) => {
                                println!("Error reading input: {}", e);
                                return;
                            }
                        }
                    }
                }
            }

            // Check for auto-merge after handling events
            // println!("[DEBUG] Checking auto-merge. Operation count: {}, Elapsed: {:?}, Timeout: {:?}", operation_count, last_activity.elapsed(), merge_timeout);
            if operation_count > 0 && last_activity.elapsed() >= merge_timeout {
                println!("\nAuto-merge triggered due to inactivity...");
                perform_merge(storage, hash_table);
                last_activity = Instant::now();
                operation_count = 0;
                print!("> ");
                io::stdout().flush().unwrap();
            }
        }
    }
}

// Returns true if the command was to exit
fn handle_command(input: &str, storage: &mut Storage, hash_table: &mut HashTable, operation_count: &mut usize, merge_interval_seconds: u64) -> bool {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return false;
    }

    match parts[0].to_lowercase().as_str() {
        "exit" | "quit" => {
            println!("Goodbye!");
            return true;
        }
        "help" => {
            show_help(merge_interval_seconds);
        }
        "stats" => {
            show_stats(storage, *operation_count);
        }
        "merge" => {
            perform_merge(storage, hash_table);
            *operation_count = 0;
        }
        "insert" => {
            if parts.len() < 3 {
                println!("Usage: insert <key> <value>");
            } else {
                let key = parts[1];
                let value = parts[2..].join(" ");
                handle_insert(storage, hash_table, key, &value);
                *operation_count += 1;
            }
        }
        "delete" => {
            if parts.len() != 2 {
                println!("Usage: delete <key>");
            } else {
                let key = parts[1];
                handle_delete(storage, hash_table, key);
                *operation_count += 1;
            }
        }
        "get" => {
            if parts.len() != 2 {
                println!("Usage: get <key>");
            } else {
                let key = parts[1];
                handle_get(storage, hash_table, key);
            }
        }
        _ => {
            println!("Unknown command: {}. Type 'help' for available commands.", parts[0]);
        }
    }
    false
}

fn show_help(merge_interval_seconds: u64) {
    println!("Available commands:");
    println!("  insert <key> <value>  - Insert or update a key-value pair");
    println!("  delete <key>          - Delete a key");
    println!("  get <key>             - Retrieve a value by key");
    println!("  stats                 - Show storage statistics");
    println!("  merge                 - Manually trigger merge operation");
    println!("  help                  - Show this help message");
    println!("  exit                  - Exit the program");
    println!("\nAuto-merge triggers after {} seconds of inactivity.", merge_interval_seconds);
}

fn show_stats(storage: &mut Storage, operation_count: usize) {
    println!("=== Storage Statistics ===");
    if let Err(e) = storage.get_storage_stats() {
        println!("Error getting storage stats: {}", e);
    }
    println!("Operations since last merge: {}", operation_count);
}

fn perform_merge(storage: &mut Storage, hash_table: &mut HashTable) {
    println!("Performing merge operation...");
    match storage.merge_inactive_files(Some(hash_table)) {
        Ok(()) => println!("✓ Merge completed successfully"),
        Err(e) => println!("✗ Merge failed: {}", e),
    }
}

fn handle_insert(storage: &mut Storage, hash_table: &mut HashTable, key: &str, value: &str) {
    match storage.write(key, value) {
        Ok((filename, offset)) => {
            let file_location = crate::FileLocation::new(filename.clone(), offset);
            hash_table.insert(key, file_location);
            println!("✓ Inserted {}: {} (file: {}, offset: {})", key, value, filename, offset);
        }
        Err(e) => println!("✗ Failed to insert {}: {}", key, e),
    }
}

fn handle_delete(storage: &mut Storage, hash_table: &mut HashTable, key: &str) {
    match storage.delete(key) {
        Ok((filename, offset)) => {
            let file_location = crate::FileLocation::new(filename.clone(), offset);
            hash_table.insert(key, file_location);
            println!("✓ Deleted {} (tombstone: file {}, offset {})", key, filename, offset);
        }
        Err(e) => println!("✗ Failed to delete {}: {}", key, e),
    }
}

fn handle_get(storage: &mut Storage, hash_table: &mut HashTable, key: &str) {
    match hash_table.get(key) {
        Some(file_location) => {
            match storage.read_value(&file_location.filename, file_location.offset) {
                Ok(value) => {
                    println!("✓ {}: {}", key, value);
                }
                Err(crate::StorageError::KeyDeleted(_)) => {
                    println!("✗ Key '{}' has been deleted", key);
                }
                Err(e) => {
                    println!("✗ Error reading {}: {}", key, e);
                }
            }
        }
        None => {
            println!("✗ Key '{}' not found", key);
        }
    }
}