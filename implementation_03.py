"""
Implementation 03: In-Place Update (Atomic Increment)

This implementation uses an atomic UPDATE statement that increments the counter
directly in the database, without reading it first. This is the CORRECT approach
for concurrent counter updates.

Key differences from previous implementations:
- No SELECT statement (no read-modify-write cycle)
- UPDATE counter = counter + 1 (atomic operation in database)
- No race condition possible
- No need for SERIALIZABLE isolation or explicit locking
- Database handles concurrency automatically

Expected behavior:
- NO value loss
- NO errors
- Correct final value (100,000)
- Much simpler code
- Good performance
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
    Worker thread that performs atomic in-place updates
    """
    global completed_iterations, failed_iterations, error_count, progress_bar
    
    # Each thread creates its own connection
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # Manual transaction control
    
    for i in range(ITERATIONS_PER_THREAD):
        cursor = conn.cursor()
        
        try:
            # ATOMIC IN-PLACE UPDATE
            # The increment happens directly in the database
            # No SELECT needed - no race condition possible!
            cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (USER_ID,))
            
            # COMMIT (separate transaction for each record)
            conn.commit()
            
            with progress_lock:
                completed_iterations += 1
                
        except Exception as e:
            conn.rollback()
            with progress_lock:
                error_count += 1
                failed_iterations += 1
            # Uncomment to see errors (should be none)
            # print(f"\nError in thread {thread_id}, iteration {i}: {e}")
        finally:
            cursor.close()
        
        # Update progress
        with progress_lock:
            if progress_bar:
                progress_bar.update(1)
    
    conn.close()


def run_test():
    """Run the in-place update test"""
    global progress_bar, completed_iterations, failed_iterations, error_count
    
    print("=" * 70)
    print("Implementation 03: In-Place Update (Atomic Increment)")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print(f"  - Method: UPDATE counter = counter + 1 (atomic)")
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
        print("     The atomic in-place update prevents race conditions.")
        print("     The database handles concurrency automatically.")
    elif lost_updates > 0:
        print("[WARNING] Unexpected value loss detected.")
        print("          This should not happen with atomic updates.")
    
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
    filename = "_implementation_03_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 03: In-Place Update (Atomic Increment)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
        f.write(f"Update method: UPDATE counter = counter + 1 (atomic)\n")
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
        
        f.write("Why This Implementation Works\n")
        f.write("-" * 70 + "\n")
        f.write("This implementation uses an ATOMIC in-place update:\n\n")
        f.write("  UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1\n\n")
        f.write("Key advantages:\n\n")
        f.write("1. NO READ-MODIFY-WRITE CYCLE\n")
        f.write("   - No SELECT statement needed\n")
        f.write("   - The increment happens entirely within the database\n")
        f.write("   - The database engine handles the read and write atomically\n\n")
        f.write("2. NO RACE CONDITION\n")
        f.write("   - Each UPDATE is atomic at the row level\n")
        f.write("   - PostgreSQL uses row-level locking automatically\n")
        f.write("   - Concurrent UPDATEs queue up and execute sequentially\n\n")
        f.write("3. NO SPECIAL ISOLATION LEVEL NEEDED\n")
        f.write("   - Works correctly with default READ COMMITTED isolation\n")
        f.write("   - No need for SERIALIZABLE\n")
        f.write("   - No serialization errors to handle\n\n")
        f.write("4. NO EXPLICIT LOCKING NEEDED\n")
        f.write("   - No SELECT FOR UPDATE required\n")
        f.write("   - Database handles locking automatically\n\n")
        f.write("5. SIMPLE CODE\n")
        f.write("   - One line of SQL\n")
        f.write("   - No retry logic needed\n")
        f.write("   - No error handling for conflicts\n\n")
        
        f.write("How PostgreSQL Handles Concurrent Updates\n")
        f.write("-" * 70 + "\n")
        f.write("When multiple transactions try to update the same row:\n\n")
        f.write("1. First transaction acquires row-level lock\n")
        f.write("2. Other transactions wait for the lock\n")
        f.write("3. Each transaction reads the LATEST committed value\n")
        f.write("4. Each transaction applies its increment\n")
        f.write("5. Result: All increments are applied correctly\n\n")
        f.write("Example with 3 concurrent transactions:\n")
        f.write("  T1: UPDATE counter = counter + 1  (counter: 0 -> 1)\n")
        f.write("  T2: waits... then UPDATE counter = counter + 1  (counter: 1 -> 2)\n")
        f.write("  T3: waits... then UPDATE counter = counter + 1  (counter: 2 -> 3)\n")
        f.write("  Final value: 3 (correct!)\n\n")
        
        f.write("COMPARISON WITH OTHER IMPLEMENTATIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Implementation 01 (Lost-update):\n")
        f.write("  - Method: SELECT + Python increment + UPDATE\n")
        f.write("  - Result: ~90% data loss\n")
        f.write("  - Errors: None (silent data loss)\n")
        f.write("  - Speed: Fast but incorrect\n\n")
        
        f.write("Implementation 02 (SERIALIZABLE):\n")
        f.write("  - Method: SELECT + Python increment + UPDATE with SERIALIZABLE\n")
        f.write("  - Result: Data loss without retry, correct with retry\n")
        f.write("  - Errors: Many serialization errors\n")
        f.write("  - Speed: Slow due to retries\n\n")
        
        f.write("Implementation 03 (In-place update) - THIS ONE:\n")
        f.write("  - Method: Atomic UPDATE counter = counter + 1\n")
        f.write(f"  - Result: {final_value} / {expected_value} ")
        if lost_updates == 0:
            f.write("(PERFECT!)\n")
        else:
            f.write(f"({lost_updates} lost)\n")
        f.write(f"  - Errors: {error_count}\n")
        f.write(f"  - Speed: {throughput:.2f} ops/sec\n\n")
        
        f.write("PERFORMANCE COMPARISON\n")
        f.write("-" * 70 + "\n")
        f.write("Throughput comparison (operations per second):\n")
        f.write("  - Implementation 01: ~140 ops/sec (but incorrect results)\n")
        f.write("  - Implementation 02: ~50-100 ops/sec (with retries)\n")
        f.write(f"  - Implementation 03: {throughput:.2f} ops/sec (THIS ONE)\n\n")
        
        f.write("KEY TAKEAWAYS\n")
        f.write("=" * 70 + "\n\n")
        f.write("1. ATOMIC OPERATIONS ARE THE SOLUTION\n")
        f.write("   For simple counters, use: UPDATE counter = counter + 1\n")
        f.write("   This is simpler, faster, and more reliable than complex locking.\n\n")
        f.write("2. LET THE DATABASE DO THE WORK\n")
        f.write("   Don't read-modify-write in your application.\n")
        f.write("   Let the database handle the increment atomically.\n\n")
        f.write("3. SIMPLICITY WINS\n")
        f.write("   The simplest solution (one line of SQL) is often the best.\n")
        f.write("   No retry logic, no error handling, no complex isolation levels.\n\n")
        f.write("4. PERFORMANCE AND CORRECTNESS\n")
        f.write("   This approach provides both correct results AND good performance.\n")
        f.write("   You don't have to choose between speed and correctness.\n\n")
        f.write("5. REAL-WORLD APPLICATIONS\n")
        f.write("   This pattern is used everywhere:\n")
        f.write("   - Social media likes (UPDATE likes = likes + 1)\n")
        f.write("   - Video views (UPDATE views = views + 1)\n")
        f.write("   - Inventory (UPDATE stock = stock - quantity)\n")
        f.write("   - Banking (UPDATE balance = balance + amount)\n\n")
        
        f.write("BEST PRACTICES\n")
        f.write("-" * 70 + "\n")
        f.write("When implementing counters or similar features:\n\n")
        f.write("1. Use atomic updates whenever possible\n")
        f.write("2. Avoid read-modify-write patterns in application code\n")
        f.write("3. Let the database handle concurrency\n")
        f.write("4. Keep transactions short\n")
        f.write("5. Test with concurrent load\n\n")
        
        f.write("SQL PATTERNS TO USE\n")
        f.write("-" * 70 + "\n")
        f.write("Increment:\n")
        f.write("  UPDATE table SET counter = counter + 1 WHERE id = ?\n\n")
        f.write("Decrement:\n")
        f.write("  UPDATE table SET counter = counter - 1 WHERE id = ?\n\n")
        f.write("Add value:\n")
        f.write("  UPDATE table SET counter = counter + ? WHERE id = ?\n\n")
        f.write("With return value:\n")
        f.write("  UPDATE table SET counter = counter + 1 WHERE id = ? RETURNING counter\n\n")
        
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
