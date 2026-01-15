"""
Implementation 04: Row-Level Locking with SELECT ... FOR UPDATE

This implementation uses PostgreSQL's row-level locking mechanism to prevent
race conditions. The SELECT ... FOR UPDATE statement locks the row until the
transaction commits, preventing concurrent access.

Key features:
- SELECT ... FOR UPDATE acquires an exclusive row lock
- Other transactions wait for the lock to be released
- No race condition possible (row is locked)
- No SERIALIZABLE isolation needed (works with READ COMMITTED)
- No retry logic needed (transactions wait, don't fail)

How it works:
1. SELECT ... FOR UPDATE locks the row
2. Other transactions trying to lock the same row WAIT
3. Read counter, increment in Python, UPDATE
4. COMMIT releases the lock
5. Next waiting transaction acquires the lock and proceeds

Expected behavior:
- NO value loss (100% correct)
- NO errors
- Correct final value (100,000)
- Slower than atomic update due to lock contention
- Faster than SERIALIZABLE with retries
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

# Global tracking
progress_lock = threading.Lock()
total_iterations = NUM_THREADS * ITERATIONS_PER_THREAD
completed_iterations = 0
failed_iterations = 0
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
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else 0


def worker_thread(thread_id):
    """
    Worker thread that performs updates with row-level locking (SELECT ... FOR UPDATE)
    Each thread MUST have its own connection for the locking to work properly
    """
    global completed_iterations, failed_iterations, error_count, progress_bar
    
    # IMPORTANT: Each thread creates its own connection
    # This is required for SELECT ... FOR UPDATE to work correctly
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # Manual transaction control
    
    for i in range(ITERATIONS_PER_THREAD):
        cursor = conn.cursor()
        
        try:
            # Step 1: SELECT ... FOR UPDATE (acquires exclusive row lock)
            # This blocks other transactions from reading this row until COMMIT
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE", (USER_ID,))
            result = cursor.fetchone()
            counter = result[0] if result else 0
            
            # Step 2: Increment in Python
            # While we do this, other transactions are WAITING for the lock
            counter = counter + 1
            
            # Step 3: UPDATE with new value
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
            
            # Step 4: COMMIT (releases the lock)
            # Now the next waiting transaction can acquire the lock
            conn.commit()
            
            with progress_lock:
                completed_iterations += 1
                
        except Exception as e:
            conn.rollback()
            with progress_lock:
                error_count += 1
                failed_iterations += 1
            print(f"\n[ERROR] Thread {thread_id}, iteration {i}: {type(e).__name__}: {e}")
        finally:
            cursor.close()
        
        # Update progress
        with progress_lock:
            if progress_bar:
                progress_bar.update(1)
    
    conn.close()


def run_test():
    """Run the row-level locking test"""
    global progress_bar, completed_iterations, failed_iterations, error_count
    
    print("=" * 70)
    print("Implementation 04: Row-Level Locking (SELECT ... FOR UPDATE)")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print(f"  - Method: SELECT ... FOR UPDATE (row-level locking)")
    print(f"  - Each thread has its own database connection")
    print()
    
    # Reset counter
    reset_counter()
    initial_value = get_counter_value()
    print(f"Initial counter value: {initial_value}")
    print()
    
    # Reset global counters
    completed_iterations = 0
    failed_iterations = 0
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
    final_value = get_counter_value()
    
    # Calculate statistics
    expected_value = initial_value + total_iterations
    lost_updates = expected_value - final_value
    loss_percentage = (lost_updates / total_iterations) * 100 if total_iterations > 0 else 0
    throughput = total_iterations / elapsed_time
    success_rate = (completed_iterations / total_iterations) * 100 if total_iterations > 0 else 0
    
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
    print(f"Initial counter value: {initial_value}")
    print(f"Final counter value: {final_value}")
    print(f"Expected counter value: {expected_value}")
    print(f"Lost updates: {lost_updates} ({loss_percentage:.2f}%)")
    print()
    print("OPERATION STATISTICS")
    print("-" * 70)
    print(f"Successful operations: {completed_iterations:,} ({success_rate:.2f}%)")
    print(f"Failed operations: {failed_iterations:,}")
    print(f"Errors: {error_count:,}")
    print()
    
    if lost_updates == 0 and completed_iterations == total_iterations:
        print("[OK] PERFECT! All updates applied successfully.")
        print("     Row-level locking prevented all race conditions.")
        print("     Transactions waited for locks instead of failing.")
    elif lost_updates > 0:
        print("[WARNING] Unexpected value loss detected.")
        print("          This should not happen with SELECT ... FOR UPDATE.")
    
    if error_count > 0:
        print(f"[WARNING] {error_count} errors occurred during execution.")
    
    print("=" * 70)
    
    # Save results to file
    save_results(start_datetime, end_datetime, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, error_count)
    
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
        'error_count': error_count
    }


def save_results(start_time, end_time, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, error_count):
    """Save test results to file"""
    filename = "_implementation_04_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 04: Row-Level Locking (SELECT ... FOR UPDATE)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
        f.write(f"Locking method: SELECT ... FOR UPDATE (row-level exclusive lock)\n")
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
        f.write(f"Expected counter value: {expected_value}\n")
        f.write(f"Lost updates: {lost_updates}\n")
        f.write(f"Loss percentage: {loss_percentage:.2f}%\n")
        f.write("\n")
        
        f.write("OPERATION STATISTICS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Successful operations: {completed_iterations:,}\n")
        f.write(f"Failed operations: {failed_iterations:,}\n")
        f.write(f"Errors encountered: {error_count:,}\n")
        success_rate = (completed_iterations / total_iterations) * 100 if total_iterations > 0 else 0
        f.write(f"Success rate: {success_rate:.2f}%\n")
        f.write("\n")
        
        f.write("ANALYSIS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("How SELECT ... FOR UPDATE Works\n")
        f.write("-" * 70 + "\n")
        f.write("SELECT ... FOR UPDATE is PostgreSQL's row-level locking mechanism:\n\n")
        f.write("SQL Statement:\n")
        f.write("  SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE\n\n")
        f.write("What happens:\n\n")
        f.write("1. LOCK ACQUISITION\n")
        f.write("   - The SELECT acquires an exclusive lock on the matching row\n")
        f.write("   - This lock prevents other transactions from reading or writing\n")
        f.write("   - The lock is held until COMMIT or ROLLBACK\n\n")
        f.write("2. BLOCKING BEHAVIOR\n")
        f.write("   - If another transaction tries to SELECT ... FOR UPDATE the same row,\n")
        f.write("     it WAITS (blocks) until the first transaction commits\n")
        f.write("   - No error is thrown - transactions queue up and wait their turn\n\n")
        f.write("3. SEQUENTIAL EXECUTION\n")
        f.write("   - Even though we have 10 concurrent threads,\n")
        f.write("     they execute the critical section sequentially\n")
        f.write("   - Each thread waits for the previous one to commit\n\n")
        f.write("4. GUARANTEED CONSISTENCY\n")
        f.write("   - Each transaction reads the LATEST committed value\n")
        f.write("   - No race condition possible\n")
        f.write("   - All increments are applied correctly\n\n")
        
        f.write("Why This Implementation Works\n")
        f.write("-" * 70 + "\n")
        f.write("Key advantages of SELECT ... FOR UPDATE:\n\n")
        f.write("1. EXPLICIT LOCKING\n")
        f.write("   - Developer controls exactly when and what to lock\n")
        f.write("   - Lock is acquired during SELECT, not UPDATE\n")
        f.write("   - Protects the entire read-modify-write cycle\n\n")
        f.write("2. NO SERIALIZATION ERRORS\n")
        f.write("   - Unlike SERIALIZABLE isolation, no errors are thrown\n")
        f.write("   - Transactions wait instead of failing\n")
        f.write("   - No retry logic needed\n\n")
        f.write("3. WORKS WITH READ COMMITTED\n")
        f.write("   - No need for SERIALIZABLE isolation level\n")
        f.write("   - Default isolation level (READ COMMITTED) is sufficient\n")
        f.write("   - Lower overhead than SERIALIZABLE\n\n")
        f.write("4. GUARANTEED CORRECTNESS\n")
        f.write("   - Each transaction sees the latest value\n")
        f.write("   - No lost updates\n")
        f.write("   - Simpler than SERIALIZABLE + retry\n\n")
        
        f.write("Transaction Flow Example\n")
        f.write("-" * 70 + "\n")
        f.write("Time | Thread 1              | Thread 2              | Counter\n")
        f.write("-----|----------------------|----------------------|--------\n")
        f.write("t1   | SELECT FOR UPDATE    | -                    | 100\n")
        f.write("t2   | (lock acquired)      | SELECT FOR UPDATE    | 100\n")
        f.write("t3   | counter = 101        | (waiting for lock)   | 100\n")
        f.write("t4   | UPDATE counter=101   | (waiting...)         | 100\n")
        f.write("t5   | COMMIT               | (waiting...)         | 101\n")
        f.write("t6   | (lock released)      | (lock acquired!)     | 101\n")
        f.write("t7   | -                    | counter = 102        | 101\n")
        f.write("t8   | -                    | UPDATE counter=102   | 101\n")
        f.write("t9   | -                    | COMMIT               | 102\n\n")
        f.write("Result: Both increments applied correctly (100 -> 101 -> 102)\n\n")
        
        f.write("Important Requirements\n")
        f.write("-" * 70 + "\n")
        f.write("For SELECT ... FOR UPDATE to work correctly:\n\n")
        f.write("1. SEPARATE CONNECTION PER THREAD\n")
        f.write("   - Each thread MUST have its own database connection\n")
        f.write("   - Sharing connections breaks the locking mechanism\n")
        f.write("   - We implement this correctly in our code\n\n")
        f.write("2. MANUAL TRANSACTION CONTROL\n")
        f.write("   - Must disable autocommit\n")
        f.write("   - Must explicitly call COMMIT or ROLLBACK\n")
        f.write("   - Lock is held until COMMIT\n\n")
        f.write("3. PROPER ERROR HANDLING\n")
        f.write("   - Always ROLLBACK on error\n")
        f.write("   - Otherwise lock is held indefinitely (until timeout)\n")
        f.write("   - Can cause deadlocks if not careful\n\n")
        
        f.write("COMPARISON WITH OTHER IMPLEMENTATIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Implementation 01 (Lost-update):\n")
        f.write("  - Method: SELECT + Python increment + UPDATE\n")
        f.write("  - Locking: None\n")
        f.write("  - Result: ~90% data loss\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~140 ops/sec\n")
        f.write("  - Code complexity: Low\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 (SERIALIZABLE without retry):\n")
        f.write("  - Method: SERIALIZABLE isolation\n")
        f.write("  - Locking: Automatic conflict detection\n")
        f.write("  - Result: Data loss\n")
        f.write("  - Errors: Many serialization errors\n")
        f.write("  - Speed: Variable\n")
        f.write("  - Code complexity: Medium\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 WITH RETRY:\n")
        f.write("  - Method: SERIALIZABLE + automatic retry\n")
        f.write("  - Locking: Automatic conflict detection\n")
        f.write("  - Result: 92-100% correct (depends on max retries)\n")
        f.write("  - Errors: Many (handled)\n")
        f.write("  - Speed: ~23 ops/sec (very slow)\n")
        f.write("  - Code complexity: High\n")
        f.write("  - Correctness: CORRECT (with enough retries)\n\n")
        
        f.write("Implementation 03 (Atomic in-place):\n")
        f.write("  - Method: UPDATE counter = counter + 1\n")
        f.write("  - Locking: Automatic row-level locking\n")
        f.write("  - Result: 100% correct\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~122 ops/sec\n")
        f.write("  - Code complexity: Very low (1 line)\n")
        f.write("  - Correctness: CORRECT\n\n")
        
        f.write("Implementation 04 (SELECT FOR UPDATE) - THIS ONE:\n")
        f.write("  - Method: Explicit row-level locking\n")
        f.write("  - Locking: SELECT ... FOR UPDATE\n")
        if lost_updates == 0:
            f.write("  - Result: 100% correct\n")
        else:
            f.write(f"  - Result: {100-loss_percentage:.2f}% correct\n")
        f.write(f"  - Errors: {error_count}\n")
        f.write(f"  - Speed: {throughput:.2f} ops/sec\n")
        f.write("  - Code complexity: Low-Medium\n")
        if lost_updates == 0:
            f.write("  - Correctness: CORRECT\n\n")
        else:
            f.write("  - Correctness: Mostly correct\n\n")
        
        f.write("PERFORMANCE COMPARISON\n")
        f.write("-" * 70 + "\n")
        f.write("Throughput (operations per second):\n\n")
        f.write("  Implementation 01 (Lost-update):        ~140 ops/sec (WRONG)\n")
        f.write("  Implementation 02 (SERIALIZABLE):       ~100 ops/sec (WRONG)\n")
        f.write("  Implementation 02 WITH RETRY:           ~23 ops/sec  (CORRECT)\n")
        f.write("  Implementation 03 (Atomic):             ~122 ops/sec (CORRECT)\n")
        f.write(f"  Implementation 04 (SELECT FOR UPDATE): {throughput:>6.2f} ops/sec ")
        if lost_updates == 0:
            f.write("(CORRECT)\n\n")
        else:
            f.write("(MOSTLY CORRECT)\n\n")
        
        f.write("Why is Implementation 04 slower than Implementation 03?\n\n")
        f.write("1. READ-MODIFY-WRITE CYCLE\n")
        f.write("   - Implementation 04: SELECT, increment in Python, UPDATE\n")
        f.write("   - Implementation 03: Single atomic UPDATE\n")
        f.write("   - More round-trips = slower\n\n")
        f.write("2. NETWORK OVERHEAD\n")
        f.write("   - Implementation 04: Two SQL statements per increment\n")
        f.write("   - Implementation 03: One SQL statement per increment\n\n")
        f.write("3. LOCK DURATION\n")
        f.write("   - Implementation 04: Lock held during SELECT, Python code, UPDATE\n")
        f.write("   - Implementation 03: Lock only during UPDATE\n")
        f.write("   - Longer lock duration = more contention\n\n")
        
        f.write("KEY TAKEAWAYS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("1. SELECT ... FOR UPDATE IS A VALID SOLUTION\n")
        f.write("   - Provides explicit control over locking\n")
        f.write("   - Guaranteed correctness\n")
        f.write("   - No retry logic needed\n\n")
        
        f.write("2. USE WHEN YOU NEED READ-MODIFY-WRITE\n")
        f.write("   - If you need to read data, perform complex logic, then write back\n")
        f.write("   - Better than SERIALIZABLE + retry for simpler code\n")
        f.write("   - More explicit than relying on isolation levels\n\n")
        
        f.write("3. NOT IDEAL FOR SIMPLE COUNTERS\n")
        f.write("   - Implementation 03 (atomic UPDATE) is simpler and faster\n")
        f.write("   - Only use SELECT ... FOR UPDATE when you need complex logic\n")
        f.write("   - The read-modify-write cycle has overhead\n\n")
        
        f.write("4. PROPER IMPLEMENTATION IS CRITICAL\n")
        f.write("   - Each thread needs its own connection\n")
        f.write("   - Must use manual transaction control\n")
        f.write("   - Must handle errors properly (ROLLBACK)\n")
        f.write("   - Can cause deadlocks if used incorrectly\n\n")
        
        f.write("5. COMPARE TO OTHER APPROACHES\n")
        f.write("   - Simpler than SERIALIZABLE + retry\n")
        f.write("   - More complex than atomic UPDATE\n")
        f.write("   - Faster than SERIALIZABLE + retry\n")
        f.write("   - Slower than atomic UPDATE\n\n")
        
        f.write("WHEN TO USE SELECT ... FOR UPDATE\n")
        f.write("-" * 70 + "\n")
        f.write("Good use cases:\n")
        f.write("- Inventory management with business rules\n")
        f.write("- Order processing with validation\n")
        f.write("- Reservations with availability checks\n")
        f.write("- Account transfers with balance checks\n\n")
        f.write("NOT good for:\n")
        f.write("- Simple counters (use atomic UPDATE instead)\n")
        f.write("- High-contention scenarios (too much waiting)\n")
        f.write("- Read-only queries (unnecessary locking)\n\n")
        
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
