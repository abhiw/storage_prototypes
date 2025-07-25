#!/bin/bash

# Test script for the Data Intensive Applications project

# --- Configuration ---
APP_PATH="./target/debug/data_intensive_applications"
STORAGE_DIR="storage"
LOG_FILE="test_output.log"
FIFO_PIPE="/tmp/app_pipe_$$"

# --- Helper Functions ---
cleanup() {
    echo "Cleaning up..."
    rm -rf "$STORAGE_DIR"
    rm -f "$LOG_FILE"
    rm -f "$FIFO_PIPE"
    if [ ! -z "$APP_PID" ]; then
        kill -- -$APP_PID 2>/dev/null
    fi
}

trap cleanup EXIT

start_app() {
    echo "Starting the application..."
    mkfifo "$FIFO_PIPE"
    set -m
    ("$APP_PATH" < "$FIFO_PIPE" > "$LOG_FILE" 2>&1) &
    APP_PID=$!
    set +m
    #sleep 2
}

send_command() {
#    echo "Sending command: $1"
    echo "$1" >&3
    #sleep 0.5
}

# --- Test Scenarios ---
run_tests() {
    echo "--- Running Tests ---"

    # 1. Basic Operations
    send_command "insert key1 value1"
    send_command "get key1"
    send_command "delete key1"
    send_command "stats"
    send_command "help"

    # 2. Data Updates
    send_command "insert key2 value2"
    send_command "insert key2 value2_updated"
    send_command "get key2"

    # 3. File Rotation
    for i in {1..20}; do
        send_command "insert key_long_$i $(head -c 50 /dev/urandom | base64)"
    done

    # 4. Manual Merging
    send_command "merge"
    send_command "get key_long_10"

    # 5. Automatic Merging
#    echo "Waiting for auto-merge to trigger (35 seconds)..."
    sleep 35
    send_command "get key_long_15"

    echo "--- Tests Finished ---"
}

# --- Main Execution ---
echo "Building the application..."
cargo build

if [ $? -ne 0 ]; then
    echo "✗ Build failed. Aborting tests."
    exit 1
fi

start_app

# Open the FIFO for writing once
exec 3>"$FIFO_PIPE"

run_tests
send_command "exit"
#sleep 2

# Close the FIFO
exec 3>&-

exit 0
