"""
Implementation 05: Optimistic Concurrency Control (OCC)

This implementation uses the version field to implement optimistic locking.
Instead of locking rows pessimistically (like SELECT FOR UPDATE), we optimistically
assume no conflicts will occur. We detect conflicts using the version field and
retry when conflicts are detected.

Key features:
- Read counter AND version
- Increment counter in Python
- UPDATE with new values, BUT only if version hasn't changed
- Check rowcount to detect conflicts
- If conflict detected (rowcount = 0), retry
- If update succeeded (rowcount = 1), done

How it works:
1. SELECT counter, version (no lock)
2. Increment counter in Python
3. UPDATE ... WHERE user_id = ? AND version = old_version
4. If rowcount = 0: someone else updated it, retry from step 1
5. If rowcount = 1: success!

Advantages:
- No pessimistic locks (better concurrency for low contention)
- No database errors (application handles conflicts)
- Works with any isolation level
- Good for scenarios where conflicts are rare

Disadvantages:
- Requires version field in table
- Retry logic needed in application
- Can have many retries under high contention
- More complex than atomic updates
"""

import psycopg2
import threading
import time
from datetime import datetime
from tqdm import tqdm

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'counter_db',
    'user': 'counter_user',
    'password': 'counter_password'
}

# Configuration
NUM_THREADS = 10
ITERATIONS_PER_THREAD = 10_000
USER_ID = 1
MAX_RETRIES = 100  # Maximum retry attempts for each operation

# Global tracking
progress_lock = threading.Lock()
total_iterations = NUM_THREADS * ITERATIONS_PER_THREAD
completed_iterations = 0
failed_iterations = 0
total_retries = 0
max_retries_reached = 0
error_count = 0
progress_bar = None


def reset_counter():
    """Reset the counter to 0 before starting the test"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (USER_ID,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[OK] Counter reset to 0 for user_id = {USER_ID}")


def get_counter_value():
    """Get the final counter value"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (USER_ID,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result if result else (0, 0)


def worker_thread(thread_id):
    """
    Worker thread that performs updates with optimistic concurrency control
    """
    global completed_iterations, failed_iterations, total_retries, max_retries_reached, error_count, progress_bar
    
    # Each thread creates its own connection
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # Manual transaction control
    
    for i in range(ITERATIONS_PER_THREAD):
        success = False
        attempts = 0
        
        while not success and attempts < MAX_RETRIES:
            attempts += 1
            cursor = conn.cursor()
            
            try:
                # Step 1: SELECT counter AND version (no lock - optimistic approach)
                cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (USER_ID,))
                result = cursor.fetchone()
                counter = result[0] if result else 0
                version = result[1] if result else 0
                
                # Step 2: Increment in Python
                counter = counter + 1
                new_version = version + 1
                
                # Step 3: UPDATE with version check (optimistic locking)
                # This will only update if the version hasn't changed
                cursor.execute(
                    "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                    (counter, new_version, USER_ID, version)
                )
                
                # Step 4: COMMIT
                conn.commit()
                
                # Step 5: Check if update succeeded
                if cursor.rowcount > 0:
                    # Success! The version matched, update was applied
                    success = True
                    with progress_lock:
                        completed_iterations += 1
                        if attempts > 1:
                            total_retries += (attempts - 1)
                else:
                    # Conflict detected! Version changed between SELECT and UPDATE
                    # Another transaction updated the row
                    # Retry the operation
                    if attempts >= MAX_RETRIES:
                        with progress_lock:
                            failed_iterations += 1
                            max_retries_reached += 1
                        success = True  # Exit loop
                        print(f"\n[WARNING] Thread {thread_id}, iteration {i}: Max retries reached!")
                    # Otherwise, loop will retry
                        
            except Exception as e:
                conn.rollback()
                with progress_lock:
                    error_count += 1
                    failed_iterations += 1
                print(f"\n[ERROR] Thread {thread_id}, iteration {i}: {type(e).__name__}: {e}")
                success = True  # Exit retry loop
                
            finally:
                cursor.close()
        
        # Update progress
        with progress_lock:
            if progress_bar:
                progress_bar.update(1)
    
    conn.close()


def run_test():
    """Run the optimistic concurrency control test"""
    global progress_bar, completed_iterations, failed_iterations, total_retries, max_retries_reached, error_count
    
    print("=" * 70)
    print("Implementation 05: Optimistic Concurrency Control (OCC)")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print(f"  - Method: Optimistic locking with version field")
    print(f"  - Max retries per operation: {MAX_RETRIES}")
    print()
    
    # Reset counter
    reset_counter()
    initial_value, initial_version = get_counter_value()
    print(f"Initial counter value: {initial_value}, version: {initial_version}")
    print()
    
    # Reset global counters
    completed_iterations = 0
    failed_iterations = 0
    total_retries = 0
    max_retries_reached = 0
    error_count = 0
    
    # Create progress bar
    progress_bar = tqdm(total=total_iterations, desc="Processing", unit="ops")
    
    # Record start time
    start_time = time.time()
    start_datetime = datetime.now()
    
    # Create and start threads
    threads = []
    for thread_id in range(NUM_THREADS):
        thread = threading.Thread(target=worker_thread, args=(thread_id,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Close progress bar
    progress_bar.close()
    
    # Record end time
    end_time = time.time()
    end_datetime = datetime.now()
    elapsed_time = end_time - start_time
    
    # Get final counter value
    final_value, final_version = get_counter_value()
    
    # Calculate statistics
    expected_value = initial_value + total_iterations
    lost_updates = expected_value - final_value
    loss_percentage = (lost_updates / total_iterations) * 100 if total_iterations > 0 else 0
    throughput = total_iterations / elapsed_time
    success_rate = (completed_iterations / total_iterations) * 100 if total_iterations > 0 else 0
    avg_retries = total_retries / completed_iterations if completed_iterations > 0 else 0
    
    # Print results
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time: {elapsed_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} operations/second")
    print()
    print(f"Initial counter value: {initial_value}, version: {initial_version}")
    print(f"Final counter value: {final_value}, version: {final_version}")
    print(f"Expected counter value: {expected_value}")
    print(f"Lost updates: {lost_updates} ({loss_percentage:.2f}%)")
    print()
    print("OPERATION STATISTICS")
    print("-" * 70)
    print(f"Successful operations: {completed_iterations:,} ({success_rate:.2f}%)")
    print(f"Failed operations: {failed_iterations:,}")
    print(f"Total retries: {total_retries:,}")
    print(f"Average retries per success: {avg_retries:.2f}")
    print(f"Max retries reached: {max_retries_reached:,}")
    print(f"Errors: {error_count:,}")
    print()
    
    if lost_updates == 0 and completed_iterations == total_iterations:
        print("[OK] PERFECT! All updates applied successfully.")
        print("     Optimistic concurrency control prevented all conflicts.")
        print(f"     Required {total_retries:,} retries to achieve correctness.")
    elif lost_updates > 0:
        print("[WARNING] Some value loss detected.")
        print(f"          {failed_iterations} operations failed after max retries.")
    
    if error_count > 0:
        print(f"[WARNING] {error_count} errors occurred during execution.")
    
    print("=" * 70)
    
    # Save results to file
    save_results(start_datetime, end_datetime, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, total_retries, avg_retries, 
                 max_retries_reached, error_count, final_version)
    
    return {
        'elapsed_time': elapsed_time,
        'throughput': throughput,
        'initial_value': initial_value,
        'final_value': final_value,
        'expected_value': expected_value,
        'lost_updates': lost_updates,
        'loss_percentage': loss_percentage,
        'completed_iterations': completed_iterations,
        'failed_iterations': failed_iterations,
        'total_retries': total_retries,
        'avg_retries': avg_retries,
        'max_retries_reached': max_retries_reached,
        'error_count': error_count,
        'final_version': final_version
    }


def save_results(start_time, end_time, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, total_retries, avg_retries,
                 max_retries_reached, error_count, final_version):
    """Save test results to file"""
    filename = "_implementation_05_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 05: Optimistic Concurrency Control (OCC)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
        f.write(f"Method: Optimistic Concurrency Control with version field\n")
        f.write(f"Max retries per operation: {MAX_RETRIES}\n")
        f.write(f"Database: {DB_CONFIG['database']}\n")
        f.write(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}\n")
        f.write("\n")
        
        f.write("EXECUTION DETAILS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
        f.write(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
        f.write(f"Execution time: {elapsed_time:.2f} seconds\n")
        f.write(f"Throughput: {throughput:.2f} operations/second\n")
        f.write("\n")
        
        f.write("COUNTER VALUES\n")
        f.write("-" * 70 + "\n")
        f.write(f"Initial counter value: {initial_value}\n")
        f.write(f"Final counter value: {final_value}\n")
        f.write(f"Final version: {final_version}\n")
        f.write(f"Expected counter value: {expected_value}\n")
        f.write(f"Lost updates: {lost_updates}\n")
        f.write(f"Loss percentage: {loss_percentage:.2f}%\n")
        f.write("\n")
        
        f.write("OPERATION STATISTICS AND RETRY ANALYSIS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Successful operations: {completed_iterations:,}\n")
        f.write(f"Failed operations: {failed_iterations:,}\n")
        f.write(f"Total retries performed: {total_retries:,}\n")
        f.write(f"Average retries per successful operation: {avg_retries:.2f}\n")
        f.write(f"Operations that reached max retries: {max_retries_reached:,}\n")
        f.write(f"Errors encountered: {error_count:,}\n")
        success_rate = (completed_iterations / total_iterations) * 100 if total_iterations > 0 else 0
        f.write(f"Success rate: {success_rate:.2f}%\n")
        retry_overhead_pct = (total_retries / total_iterations * 100) if total_iterations > 0 else 0
        f.write(f"Retry overhead: {retry_overhead_pct:.1f}%\n")
        f.write("\n")
        
        f.write("ANALYSIS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("How Optimistic Concurrency Control Works\n")
        f.write("-" * 70 + "\n")
        f.write("Optimistic locking assumes conflicts are rare and detects them when\n")
        f.write("they occur, rather than preventing them with locks.\n\n")
        f.write("The Version Field Approach:\n\n")
        f.write("1. READ phase (no locks):\n")
        f.write("   SELECT counter, version FROM user_counter WHERE user_id = 1\n\n")
        f.write("2. COMPUTE phase (in application):\n")
        f.write("   counter = counter + 1\n")
        f.write("   new_version = version + 1\n\n")
        f.write("3. WRITE phase (conditional update):\n")
        f.write("   UPDATE user_counter\n")
        f.write("   SET counter = ?, version = ?\n")
        f.write("   WHERE user_id = ? AND version = old_version\n\n")
        f.write("4. CHECK phase:\n")
        f.write("   if rowcount == 0:\n")
        f.write("       # Conflict! Someone else updated it\n")
        f.write("       # Retry from step 1\n")
        f.write("   else:\n")
        f.write("       # Success! Update was applied\n\n")
        
        f.write("Transaction Flow with Conflict\n")
        f.write("-" * 70 + "\n")
        f.write("Time | Thread 1              | Thread 2              | DB State\n")
        f.write("-----|----------------------|----------------------|----------\n")
        f.write("t1   | SELECT (c=100, v=5)  | -                    | c=100,v=5\n")
        f.write("t2   | -                    | SELECT (c=100, v=5)  | c=100,v=5\n")
        f.write("t3   | c=101, v=6 (compute) | c=101, v=6 (compute) | c=100,v=5\n")
        f.write("t4   | UPDATE WHERE v=5     | -                    | c=100,v=5\n")
        f.write("t5   | rowcount=1 SUCCESS!  | -                    | c=101,v=6\n")
        f.write("t6   | -                    | UPDATE WHERE v=5     | c=101,v=6\n")
        f.write("t7   | -                    | rowcount=0 CONFLICT! | c=101,v=6\n")
        f.write("t8   | -                    | RETRY: SELECT again  | c=101,v=6\n")
        f.write("t9   | -                    | (c=101, v=6)         | c=101,v=6\n")
        f.write("t10  | -                    | c=102, v=7 (compute) | c=101,v=6\n")
        f.write("t11  | -                    | UPDATE WHERE v=6     | c=101,v=6\n")
        f.write("t12  | -                    | rowcount=1 SUCCESS!  | c=102,v=7\n\n")
        f.write("Result: Both increments applied after Thread 2 retried\n\n")
        
        f.write("Why This Implementation Works\n")
        f.write("-" * 70 + "\n")
        f.write("Key advantages of Optimistic Concurrency Control:\n\n")
        f.write("1. NO PESSIMISTIC LOCKS\n")
        f.write("   - No SELECT FOR UPDATE needed\n")
        f.write("   - Better concurrency when conflicts are rare\n")
        f.write("   - Readers don't block writers, writers don't block readers\n\n")
        f.write("2. NO DATABASE ERRORS\n")
        f.write("   - No SerializationFailure exceptions\n")
        f.write("   - Conflicts detected via rowcount, not exceptions\n")
        f.write("   - Application has full control over retry logic\n\n")
        f.write("3. WORKS WITH ANY ISOLATION LEVEL\n")
        f.write("   - No need for SERIALIZABLE\n")
        f.write("   - Works fine with READ COMMITTED (default)\n")
        f.write("   - Lower database overhead\n\n")
        f.write("4. APPLICATION-LEVEL RETRY\n")
        f.write("   - Application decides when and how to retry\n")
        f.write("   - Can implement custom backoff strategies\n")
        f.write("   - Can log conflicts for monitoring\n\n")
        f.write("5. VERSION FIELD PROVIDES AUDIT TRAIL\n")
        f.write("   - Can track how many times a row has been updated\n")
        f.write("   - Useful for debugging and monitoring\n\n")
        
        f.write("Trade-offs: Optimistic vs Pessimistic Locking\n")
        f.write("-" * 70 + "\n")
        f.write("OPTIMISTIC (this implementation):\n")
        f.write("  + Better for LOW contention scenarios\n")
        f.write("  + No lock waiting time\n")
        f.write("  + Better read concurrency\n")
        f.write("  - More retries under HIGH contention\n")
        f.write("  - Wasted work when conflicts occur\n")
        f.write("  - Requires version field in schema\n\n")
        f.write("PESSIMISTIC (SELECT FOR UPDATE):\n")
        f.write("  + Better for HIGH contention scenarios\n")
        f.write("  + No wasted work (operations wait, don't retry)\n")
        f.write("  + Simpler (no version field needed)\n")
        f.write("  - Threads wait for locks (can be slow)\n")
        f.write("  - Readers and writers block each other\n")
        f.write("  - Potential for deadlocks\n\n")
        
        f.write(f"RESULTS FOR THIS TEST\n")
        f.write("-" * 70 + "\n")
        if lost_updates == 0:
            f.write(f"SUCCESS: All {total_iterations:,} updates applied correctly\n")
        else:
            f.write(f"PARTIAL SUCCESS: {completed_iterations:,}/{total_iterations:,} updates applied\n")
        f.write(f"Total retries needed: {total_retries:,}\n")
        f.write(f"Average {avg_retries:.2f} retries per successful operation\n")
        f.write(f"Retry overhead: {retry_overhead_pct:.1f}% extra operations\n\n")
        if avg_retries < 1.0:
            f.write("Low retry rate indicates acceptable contention level.\n")
        elif avg_retries < 3.0:
            f.write("Moderate retry rate - OCC is still effective.\n")
        else:
            f.write("High retry rate - consider pessimistic locking for this workload.\n")
        f.write("\n")
        
        f.write("COMPARISON WITH OTHER IMPLEMENTATIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Implementation 01 (Lost-update):\n")
        f.write("  - Method: SELECT + UPDATE (no protection)\n")
        f.write("  - Result: ~90% data loss\n")
        f.write("  - Retries: None\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~140 ops/sec\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 (SERIALIZABLE without retry):\n")
        f.write("  - Method: SERIALIZABLE isolation\n")
        f.write("  - Result: Data loss\n")
        f.write("  - Retries: None\n")
        f.write("  - Errors: Many SerializationFailure\n")
        f.write("  - Speed: ~100 ops/sec\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 WITH RETRY:\n")
        f.write("  - Method: SERIALIZABLE + retry\n")
        f.write("  - Result: 92-100% correct\n")
        f.write("  - Retries: Many (database-triggered)\n")
        f.write("  - Errors: Many SerializationFailure (handled)\n")
        f.write("  - Speed: ~23 ops/sec\n")
        f.write("  - Correctness: CORRECT (if enough retries)\n\n")
        
        f.write("Implementation 03 (Atomic in-place):\n")
        f.write("  - Method: UPDATE counter = counter + 1\n")
        f.write("  - Result: 100% correct\n")
        f.write("  - Retries: None needed\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~122 ops/sec\n")
        f.write("  - Correctness: CORRECT\n\n")
        
        f.write("Implementation 04 (SELECT FOR UPDATE):\n")
        f.write("  - Method: Pessimistic row locking\n")
        f.write("  - Result: 100% correct\n")
        f.write("  - Retries: None (threads wait for locks)\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~98 ops/sec\n")
        f.write("  - Correctness: CORRECT\n\n")
        
        f.write("Implementation 05 (Optimistic Locking) - THIS ONE:\n")
        f.write("  - Method: Optimistic concurrency control\n")
        if lost_updates == 0:
            f.write("  - Result: 100% correct\n")
        else:
            f.write(f"  - Result: {100-loss_percentage:.2f}% correct\n")
        f.write(f"  - Retries: {total_retries:,} (application-triggered)\n")
        f.write(f"  - Errors: {error_count}\n")
        f.write(f"  - Speed: {throughput:.2f} ops/sec\n")
        if lost_updates == 0:
            f.write("  - Correctness: CORRECT\n\n")
        else:
            f.write("  - Correctness: Mostly correct\n\n")
        
        f.write("PERFORMANCE COMPARISON\n")
        f.write("-" * 70 + "\n")
        f.write("Throughput (operations per second):\n\n")
        f.write("  1. Implementation 03 (Atomic):         ~122 ops/sec  (BEST)\n")
        f.write("  2. Implementation 01 (Lost-update):    ~140 ops/sec  (WRONG)\n")
        f.write(f"  3. Implementation 05 (OCC):            {throughput:>5.2f} ops/sec  (THIS)\n")
        f.write("  4. Implementation 04 (FOR UPDATE):     ~98 ops/sec\n")
        f.write("  5. Implementation 02 (SERIAL):         ~100 ops/sec  (WRONG)\n")
        f.write("  6. Implementation 02 WITH RETRY:       ~23 ops/sec   (SLOWEST)\n\n")
        
        f.write("KEY TAKEAWAYS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("1. OPTIMISTIC LOCKING IS A VALID APPROACH\n")
        f.write("   - Works well when conflicts are relatively rare\n")
        f.write("   - Provides good concurrency without locks\n")
        f.write("   - Application has full control over conflict resolution\n\n")
        
        f.write("2. VERSION FIELD IS THE KEY\n")
        f.write("   - Enables conflict detection without database errors\n")
        f.write("   - Each successful update increments the version\n")
        f.write("   - Failed updates don't change the version\n")
        f.write("   - Simple and effective mechanism\n\n")
        
        f.write("3. CHOOSE BASED ON CONTENTION LEVEL\n")
        f.write("   - LOW contention: Optimistic locking is efficient\n")
        f.write("   - HIGH contention: Pessimistic locking is better\n")
        f.write("   - This test has HIGH contention (10 threads, 1 row)\n")
        f.write(f"   - Result: {avg_retries:.2f} retries per success\n\n")
        
        f.write("4. STILL NOT AS GOOD AS ATOMIC UPDATES\n")
        f.write("   - For simple counters, Implementation 03 is still best\n")
        f.write("   - OCC is valuable when you need complex read-modify-write\n")
        f.write("   - Use when you can't express the operation as a single UPDATE\n\n")
        
        f.write("5. REAL-WORLD APPLICATIONS\n")
        f.write("   - Online reservation systems\n")
        f.write("   - Document editing (like Google Docs)\n")
        f.write("   - Shopping carts and inventory\n")
        f.write("   - Any scenario with occasional conflicts\n\n")
        
        f.write("WHEN TO USE OPTIMISTIC CONCURRENCY CONTROL\n")
        f.write("-" * 70 + "\n")
        f.write("Good scenarios for OCC:\n")
        f.write("- Low to moderate contention\n")
        f.write("- Read-mostly workloads\n")
        f.write("- Long-running transactions with business logic\n")
        f.write("- When lock waiting would hurt user experience\n")
        f.write("- Distributed systems where locking is expensive\n\n")
        f.write("NOT good for:\n")
        f.write("- High contention on single rows (use pessimistic locks)\n")
        f.write("- Simple counters (use atomic UPDATE)\n")
        f.write("- When retry overhead is unacceptable\n")
        f.write("- Real-time systems with strict timing requirements\n\n")
        
        f.write("=" * 70 + "\n")
    
    print(f"\n[OK] Results saved to: {filename}")


if __name__ == "__main__":
    try:
        results = run_test()
    except KeyboardInterrupt:
        print("\n\n[WARNING] Test interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Error running test: {e}")
        import traceback
        traceback.print_exc()
