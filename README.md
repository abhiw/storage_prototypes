# Data Intensive Applications

## TODO
- ~~Add crc and timestamp tp file entry structure.~~
- ~~Only value position is needed in hash table file offset along with value size and timestamp~~
- Benchmark somehow. No idea how to do it yet.
- Probably add a in memory hash table instead of using active file to write data and dump it at intervals to file.

This project is a key-value storage system designed to handle data-intensive workloads. It is built in Rust and features a log-structured storage engine, an in-memory hash table for indexing, and an efficient event loop for handling user input.

## Design

The system is composed of three main components:

*   **Storage Engine**: A log-structured storage engine that appends data to files. This design is optimized for high-throughput writes and efficient sequential reads.
*   **Hash Table**: An in-memory hash table that stores the location of each key in the storage files. This allows for fast lookups without scanning the entire storage.
*   **Event Loop**: An event loop that uses I/O multiplexing to handle user input and other events. This allows the system to handle multiple concurrent operations without blocking.

### Storage Engine

The storage engine is implemented as a log-structured merge-tree (LSM-tree). Data is appended to a series of files, and a background process merges these files to reclaim space and improve read performance.

### Hash Table

The hash table is an in-memory index that maps keys to their location in the storage files. This allows for fast lookups, but it also means that the entire index must fit in memory.

### Event Loop

The event loop is implemented using the `mio` library, which provides a low-level interface to the operating system's I/O multiplexing APIs. This allows the system to handle a large number of concurrent connections with a small number of threads.

## Building

To build the project, you will need to have Rust and Cargo installed. You can then build the project using the following command:

```bash
cargo build --release
```

This will create an optimized binary in the `target/release` directory.

## Testing

The project includes a test script that covers a variety of scenarios, including basic operations, data updates, file rotation, and merging.

To run the tests, you will need to have `socat` installed. You can then run the tests using the following command:

```bash
./test_runner.sh
```

This will run a series of tests and print a summary of the results. For more detailed information, you can inspect the `test_output.log` file, which contains the complete output of the application during the test run.
