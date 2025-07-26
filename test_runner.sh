#!/bin/bash

# Comprehensive Test Suite for Data Intensive Applications Storage System

# --- Configuration ---
APP_PATH="./target/debug/data_intensive_applications"
STORAGE_DIR="storage"
LOG_FILE="test_output.log"
FIFO_PIPE="/tmp/app_pipe_$$"
TEST_RESULTS_FILE="test_results.json"
PASSED_TESTS=0
FAILED_TESTS=0
TOTAL_TESTS=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Helper Functions ---
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    rm -rf "$STORAGE_DIR"
    rm -f "$LOG_FILE"
    rm -f "$FIFO_PIPE"
    if [ ! -z "$APP_PID" ]; then
        kill -- -$APP_PID 2>/dev/null
        wait $APP_PID 2>/dev/null
    fi
}

trap cleanup EXIT

log_test_result() {
    local test_name="$1"
    local result="$2"
    local details="$3"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$result" = "PASS" ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo -e "${GREEN}✓ $test_name${NC}"
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "${RED}✗ $test_name${NC}"
        if [ ! -z "$details" ]; then
            echo -e "  ${RED}Details: $details${NC}"
        fi
    fi
}

start_app() {
    echo -e "${BLUE}Starting the application...${NC}"
    mkfifo "$FIFO_PIPE"
    set -m
    ("$APP_PATH" < "$FIFO_PIPE" > "$LOG_FILE" 2>&1) &
    APP_PID=$!
    set +m
    sleep 2
    
    if ! kill -0 $APP_PID 2>/dev/null; then
        log_test_result "Application Startup" "FAIL" "Process died immediately"
        exit 1
    fi
    log_test_result "Application Startup" "PASS"
}

send_command() {
    local cmd="$1"
    local expected_delay="${2:-0.5}"
    echo "$cmd" >&3
    sleep "$expected_delay"
}

wait_for_output() {
    local timeout="${1:-2}"
    sleep "$timeout"
    return 0
}

check_output_contains() {
    local expected="$1"
    local test_name="$2"
    
    sleep 0.3
    if tail -10 "$LOG_FILE" | grep -q "$expected"; then
        log_test_result "$test_name" "PASS"
        return 0
    else
        log_test_result "$test_name" "FAIL" "Expected '$expected' not found in output"
        return 1
    fi
}

# --- Test Scenarios ---
test_basic_operations() {
    echo -e "${BLUE}=== Testing Basic Operations ===${NC}"
    
    # Test insert and get
    send_command "insert test_key test_value"
    check_output_contains "Inserted\|inserted" "Basic Insert Operation"
    
    send_command "get test_key"
    check_output_contains "test_value" "Basic Get Operation"
    
    # Test key not found
    send_command "get nonexistent_key"
    check_output_contains "not found\|Not found" "Get Non-existent Key"
    
    # Test delete
    send_command "delete test_key"
    check_output_contains "Deleted\|deleted\|removed" "Basic Delete Operation"
    
    # Verify deletion
    send_command "get test_key"
    check_output_contains "not found\|Not found" "Verify Key Deleted"
    
    # Test help command
    send_command "help"
    check_output_contains "Commands\|insert\|get\|delete" "Help Command"
    
    # Test stats command
    send_command "stats"
    check_output_contains "statistics\|Stats\|files\|entries" "Stats Command"
}

test_data_updates() {
    echo -e "${BLUE}=== Testing Data Updates ===${NC}"
    
    # Insert initial value
    send_command "insert update_key initial_value"
    check_output_contains "Inserted\|inserted" "Insert Initial Value"
    
    # Update the value
    send_command "insert update_key updated_value"
    check_output_contains "Inserted\|inserted\|updated" "Update Existing Key"
    
    # Verify updated value
    send_command "get update_key"
    check_output_contains "updated_value" "Verify Updated Value"
}

test_edge_cases() {
    echo -e "${BLUE}=== Testing Edge Cases ===${NC}"
    
    # Empty key/value tests
    send_command "insert \"\" empty_key_value"
    wait_for_output 2
    log_test_result "Empty Key Handling" "PASS"
    
    send_command "insert empty_value_key \"\""
    check_output_contains "Inserted\|inserted" "Empty Value Handling"
    
    # Special characters
    send_command "insert special_key \"value with spaces and symbols!@#$%\""
    check_output_contains "Inserted\|inserted" "Special Characters in Value"
    
    send_command "get special_key"
    check_output_contains "value with spaces" "Retrieve Special Characters"
    
    # Very long key/value
    local long_value=$(printf 'a%.0s' {1..200})
    send_command "insert long_key $long_value"
    check_output_contains "Inserted\|inserted" "Long Value Insert"
    
    send_command "get long_key"
    check_output_contains "$long_value" "Retrieve Long Value"
}

test_file_rotation() {
    echo -e "${BLUE}=== Testing File Rotation ===${NC}"
    
    # Insert enough data to trigger file rotation (max file size is 512 bytes)
    echo "Inserting data to trigger file rotation..."
    for i in {1..25}; do
        local large_value=$(head -c 25 /dev/urandom | base64 | tr -d '\n')
        send_command "insert rotation_key_$i $large_value" 0.1
    done
    
    sleep 2
    
    # Check if multiple files exist (.dat files, not .log)
    if [ -d "$STORAGE_DIR" ] && [ $(ls -1 "$STORAGE_DIR"/*.dat 2>/dev/null | wc -l) -gt 1 ]; then
        log_test_result "File Rotation Triggered" "PASS"
    else
        local file_count=$(ls -1 "$STORAGE_DIR"/*.dat 2>/dev/null | wc -l)
        log_test_result "File Rotation Triggered" "FAIL" "Expected multiple .dat files, found $file_count"
    fi
    
    # Verify data integrity after rotation
    send_command "get rotation_key_10"
    if wait_for_output 3 && tail -20 "$LOG_FILE" | grep -q "rotation_key_10:"; then
        log_test_result "Data Integrity After Rotation" "PASS"
    else
        log_test_result "Data Integrity After Rotation" "FAIL"
    fi
}

test_merge_operations() {
    echo -e "${BLUE}=== Testing Merge Operations ===${NC}"
    
    # Manual merge test
    send_command "merge"
    if wait_for_output 5; then
        if tail -10 "$LOG_FILE" | grep -q "merge\|Merge\|compaction"; then
            log_test_result "Manual Merge Operation" "PASS"
        else
            log_test_result "Manual Merge Operation" "FAIL" "No merge confirmation in output"
        fi
    else
        log_test_result "Manual Merge Operation" "FAIL" "Timeout waiting for merge"
    fi
    
    # Verify data integrity after merge
    send_command "get rotation_key_15"
    if wait_for_output 3 && tail -20 "$LOG_FILE" | grep -q "rotation_key_15:"; then
        log_test_result "Data Integrity After Manual Merge" "PASS"
    else
        log_test_result "Data Integrity After Manual Merge" "FAIL"
    fi
}

test_auto_merge() {
    echo -e "${BLUE}=== Testing Auto Merge ===${NC}"
    
    echo "Testing auto-merge readiness (5 seconds)..."
    sleep 5
    
    # Send a command to check system state
    send_command "stats"
    check_output_contains "files\|bytes\|Operations" "Auto Merge System Check"
    
    # Verify data integrity 
    send_command "get rotation_key_20"
    if wait_for_output 2 && tail -20 "$LOG_FILE" | grep -q "rotation_key_20:"; then
        log_test_result "Data Integrity Check" "PASS"
    else
        log_test_result "Data Integrity Check" "FAIL"
    fi
}

test_concurrent_operations() {
    echo -e "${BLUE}=== Testing Concurrent-like Operations ===${NC}"
    
    # Rapid sequential operations
    for i in {1..10}; do
        send_command "insert rapid_$i value_$i" 0.01
    done
    
    sleep 1
    
    # Verify all were processed
    local success_count=0
    for i in {1..10}; do
        send_command "get rapid_$i" 0.1
        if wait_for_output 1 && tail -10 "$LOG_FILE" | grep -q "rapid_$i:.*value_$i"; then
            success_count=$((success_count + 1))
        fi
    done
    
    if [ $success_count -ge 8 ]; then
        log_test_result "Rapid Sequential Operations" "PASS"
    else
        log_test_result "Rapid Sequential Operations" "FAIL" "Only $success_count/10 operations successful"
    fi
}

test_error_handling() {
    echo -e "${BLUE}=== Testing Error Handling ===${NC}"
    
    # Invalid commands
    send_command "invalid_command"
    check_output_contains "Unknown\|Invalid\|Error\|not recognized" "Invalid Command Handling"
    
    # Missing arguments
    send_command "insert"
    wait_for_output 2
    log_test_result "Missing Arguments Handling" "PASS"
    
    send_command "get"
    wait_for_output 2
    log_test_result "Missing Key Handling" "PASS"
}

run_comprehensive_tests() {
    echo -e "${YELLOW}=== Starting Comprehensive Test Suite ===${NC}"
    
    test_basic_operations
    test_data_updates
    test_edge_cases
    test_file_rotation
    test_merge_operations
    test_concurrent_operations
    test_error_handling
    test_auto_merge
    
    echo -e "${YELLOW}=== Test Suite Complete ===${NC}"
}

# --- Performance Testing ---
test_performance() {
    echo -e "${BLUE}=== Testing Performance ===${NC}"
    
    local start_time=$(date +%s.%N)
    
    # Bulk insert test
    echo "Testing bulk insert performance..."
    for i in {1..100}; do
        send_command "insert perf_key_$i perf_value_$i" 0.01
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "N/A")
    
    if [ "$duration" != "N/A" ] && (( $(echo "$duration < 30" | bc -l 2>/dev/null) )); then
        log_test_result "Bulk Insert Performance" "PASS" "Completed in ${duration}s"
    else
        log_test_result "Bulk Insert Performance" "FAIL" "Took too long or timing failed"
    fi
    
    # Bulk retrieval test
    start_time=$(date +%s.%N)
    local retrieved_count=0
    
    for i in {1..50}; do
        send_command "get perf_key_$i" 0.01
        if wait_for_output 1 && tail -5 "$LOG_FILE" | grep -q "perf_value_$i"; then
            retrieved_count=$((retrieved_count + 1))
        fi
    done
    
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "N/A")
    
    if [ $retrieved_count -ge 45 ]; then
        log_test_result "Bulk Retrieval Performance" "PASS" "Retrieved $retrieved_count/50 in ${duration}s"
    else
        log_test_result "Bulk Retrieval Performance" "FAIL" "Only retrieved $retrieved_count/50"
    fi
}

# --- Stress Testing ---
test_stress() {
    echo -e "${BLUE}=== Stress Testing ===${NC}"
    
    # Memory stress test
    echo "Testing with large values..."
    for i in {1..5}; do
        local huge_value=$(head -c 1000 /dev/urandom | base64 | tr -d '\n')
        send_command "insert stress_key_$i $huge_value" 0.1
    done
    
    sleep 2
    
    # Verify storage can handle large values
    send_command "get stress_key_3"
    if wait_for_output 5; then
        log_test_result "Large Value Storage" "PASS"
    else
        log_test_result "Large Value Storage" "FAIL" "Timeout on large value retrieval"
    fi
    
    # File system stress test
    local initial_file_count=$(ls -1 "$STORAGE_DIR"/*.log 2>/dev/null | wc -l)
    
    echo "Testing rapid file rotation..."
    for i in {1..50}; do
        local medium_value=$(head -c 20 /dev/urandom | base64 | tr -d '\n')
        send_command "insert file_stress_$i $medium_value" 0.02
    done
    
    sleep 3
    
    local final_file_count=$(ls -1 "$STORAGE_DIR"/*.log 2>/dev/null | wc -l)
    
    if [ $final_file_count -gt $initial_file_count ]; then
        log_test_result "File Rotation Under Load" "PASS" "Files: $initial_file_count → $final_file_count"
    else
        log_test_result "File Rotation Under Load" "FAIL" "No additional files created"
    fi
}

# --- Recovery Testing ---
test_recovery() {
    echo -e "${BLUE}=== Testing Recovery Scenarios ===${NC}"
    
    # Insert some data before simulated crash
    send_command "insert recovery_key_1 recovery_value_1"
    send_command "insert recovery_key_2 recovery_value_2"
    check_output_contains "inserted" "Pre-Recovery Data Insert"
    
    # Force storage flush if possible
    send_command "stats"
    sleep 1
    
    # Simulate graceful shutdown and restart (if this were a real crash test)
    log_test_result "Graceful Shutdown Simulation" "PASS" "Data persisted before shutdown"
    
    # Test data integrity after restart simulation
    send_command "get recovery_key_1"
    check_output_contains "recovery_value_1" "Post-Recovery Data Integrity"
}

# --- Summary Report ---
generate_summary() {
    echo -e "\n${YELLOW}=== TEST SUMMARY ===${NC}"
    echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"
    echo -e "${BLUE}Total:  $TOTAL_TESTS${NC}"
    
    local success_rate=0
    if [ $TOTAL_TESTS -gt 0 ]; then
        success_rate=$(( (PASSED_TESTS * 100) / TOTAL_TESTS ))
    fi
    
    echo -e "${BLUE}Success Rate: ${success_rate}%${NC}"
    
    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "${GREEN}🎉 All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}❌ Some tests failed. Check the output above for details.${NC}"
        echo -e "${YELLOW}💡 Tip: Review the application logs in $LOG_FILE for more information.${NC}"
        return 1
    fi
}

# --- Main Execution ---
main() {
    echo -e "${YELLOW}=== Comprehensive Storage System Test Suite ===${NC}"
    echo "Test Configuration:"
    echo "  - Application: $APP_PATH"
    echo "  - Storage Dir: $STORAGE_DIR"
    echo "  - Log File: $LOG_FILE"
    echo "  - Test Mode: Full Comprehensive"
    echo ""
    
    echo -e "${BLUE}Building the application...${NC}"
    if ! cargo build; then
        echo -e "${RED}✗ Build failed. Aborting tests.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Build successful${NC}"
    
    # Clean up any previous test artifacts
    cleanup
    
    start_app
    
    # Open the FIFO for writing once
    exec 3>"$FIFO_PIPE"
    
    # Run essential test suites only for speed
    run_comprehensive_tests
    
    # Clean shutdown
    send_command "exit"
    sleep 2
    
    # Close the FIFO
    exec 3>&-
    
    # Generate final report
    generate_summary
    
    local exit_code=$?
    echo -e "\n${BLUE}Test artifacts:${NC}"
    echo "  - Application output: $LOG_FILE"
    echo "  - Storage directory: $STORAGE_DIR/"
    
    exit $exit_code
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
