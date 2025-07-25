pub mod hash_table;
pub mod storage;

pub use hash_table::{HashTable, CollisionResolution, Entry, FileLocation};
pub use storage::{Storage, StorageError, TOMBSTONE_MARKER, HashTableTrait};