
pub mod terminal_event_loop;

use crate::{Storage, HashTable};

pub trait EventLoop {
    fn run(&mut self, storage: &mut Storage, hash_table: &mut HashTable, merge_interval_seconds: u64);
}
