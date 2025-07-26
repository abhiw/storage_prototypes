use std::fs;
use serde::Deserialize;
use data_intensive_applications::{HashTable, Storage, CollisionResolution, FileLocation, StorageError};
use crate::event_loop::terminal_event_loop::TerminalEventLoop;
use crate::event_loop::EventLoop;

mod event_loop;

#[derive(Deserialize)]
struct Config {
    storage: StorageConfig,
}

#[derive(Deserialize)]
struct StorageConfig {
    max_file_size: u64,
    directory: String,
    merge_interval_seconds: u64,
}

fn init() -> (Storage, StorageConfig) {
    let config_content = fs::read_to_string("config.toml")
        .expect("Failed to read config.toml");
    
    let config: Config = toml::from_str(&config_content)
        .expect("Failed to parse config.toml");
    
    match Storage::new_with_config(&config.storage.directory, config.storage.max_file_size) {
        Ok(storage) => {
            println!("✓ Storage initialized in '{}/' directory", config.storage.directory);
            println!("  - Max file size: {} bytes", config.storage.max_file_size);
            println!("  - Auto-merge interval: {} seconds", config.storage.merge_interval_seconds);
            (storage, config.storage)
        },
        Err(e) => {
            panic!("Failed to initialize storage: {}", e);
        }
    }
}

fn main() {
    println!("=== Interactive Hash Table Storage System ===");
    
    let (mut storage, config) = init();
    let mut hash_table = HashTable::new(127, CollisionResolution::Chaining);
    
    println!("
Entering interactive mode...");
    println!("Commands:");
    println!("  insert <key> <value>  - Insert or update a key-value pair");
    println!("  delete <key>          - Delete a key");
    println!("  get <key>             - Retrieve a value by key");
    println!("  stats                 - Show storage statistics");
    println!("  merge                 - Manually trigger merge operation");
    println!("  help                  - Show this help message");
    println!("  exit                  - Exit the program");
    println!("
Auto-merge will trigger after {} seconds of inactivity.
", config.merge_interval_seconds);
    
    let mut event_loop = TerminalEventLoop {};
    event_loop.run(&mut storage, &mut hash_table, config.merge_interval_seconds);
}