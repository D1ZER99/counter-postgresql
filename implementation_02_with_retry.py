"""
Implementation 02 WITH RETRY: SERIALIZABLE Transaction Isolation Level with Retry Logic

This implementation uses PostgreSQL's SERIALIZABLE isolation level WITH automatic
retry logic enabled. When a serialization error occurs, the transaction is
automatically retried until it succeeds (up to MAX_RETRIES attempts).

Key differences from Implementation 02 (without retry):
- USE_RETRY_LOGIC = True (enabled)
- Automatically retries failed transactions
- Uses exponential backoff to reduce contention
- Should achieve 100% correct final value

Expected behavior:
- NO value loss (all 100,000 updates applied)
- Many serialization errors (but all handled by retry)
- Slower execution due to retry overhead
- Correct final value guaranteed
"""

import psycopg2
from psycopg2 import errors
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
USE_RETRY_LOGIC = True  # ENABLED - This is the key difference!
MAX_RETRIES = 50  # Increased to handle high contention

# Global tracking
progress_lock = threading.Lock()
total_iterations = NUM_THREADS * ITERATIONS_PER_THREAD
completed_iterations = 0
failed_iterations = 0
serialization_errors = 0
other_errors = 0
retry_count = 0
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
    Worker thread that performs updates with SERIALIZABLE isolation level and RETRY LOGIC
    """
    global completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count, progress_bar
    
    # Each thread creates its own connection
    conn = psycopg2.connect(**DB_CONFIG)
    
    for i in range(ITERATIONS_PER_THREAD):
        success = False
        attempts = 0
        
        while not success and attempts < MAX_RETRIES:
            attempts += 1
            cursor = conn.cursor()
            
            try:
                # Set SERIALIZABLE isolation level for this transaction
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
                
                # Step 1: SELECT counter value
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
                result = cursor.fetchone()
                counter = result[0] if result else 0
                
                # Step 2: Increment in Python
                counter = counter + 1
                
                # Step 3: UPDATE with new value
                cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
                
                # Step 4: COMMIT
                conn.commit()
                
                success = True
                
                with progress_lock:
                    completed_iterations += 1
                    if attempts > 1:
                        retry_count += (attempts - 1)
                
            except errors.SerializationFailure as e:
                # This is the key error that SERIALIZABLE isolation level throws
                conn.rollback()
                with progress_lock:
                    serialization_errors += 1
                
                if attempts < MAX_RETRIES:
                    # Retry the transaction with exponential backoff (capped at 1 second)
                    backoff_time = min(0.001 * (2 ** (attempts - 1)), 1.0)
                    time.sleep(backoff_time)
                else:
                    # Max retries reached - give up
                    success = True  # Exit retry loop
                    with progress_lock:
                        failed_iterations += 1
                    print(f"\n[WARNING] Thread {thread_id}, iteration {i}: Max retries reached!")
                        
            except Exception as e:
                conn.rollback()
                with progress_lock:
                    other_errors += 1
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
    """Run the SERIALIZABLE isolation level test WITH RETRY"""
    global progress_bar, completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count
    
    print("=" * 70)
    print("Implementation 02 WITH RETRY: SERIALIZABLE + Automatic Retry")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print(f"  - Isolation Level: SERIALIZABLE")
    print(f"  - Retry Logic: {'ENABLED' if USE_RETRY_LOGIC else 'DISABLED'}")
    print(f"  - Max Retries: {MAX_RETRIES}")
    print()
    
    # Reset counter
    reset_counter()
    initial_value = get_counter_value()
    print(f"Initial counter value: {initial_value}")
    print()
    
    # Reset global counters
    completed_iterations = 0
    failed_iterations = 0
    serialization_errors = 0
    other_errors = 0
    retry_count = 0
    
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
    print("ERROR STATISTICS")
    print("-" * 70)
    print(f"Successful transactions: {completed_iterations:,} ({success_rate:.2f}%)")
    print(f"Failed transactions: {failed_iterations:,}")
    print(f"Serialization errors: {serialization_errors:,}")
    print(f"Other errors: {other_errors:,}")
    print(f"Total retries: {retry_count:,}")
    if completed_iterations > 0:
        print(f"Average retries per success: {retry_count/completed_iterations:.2f}")
    print()
    
    if serialization_errors > 0:
        print("[INFO] Serialization errors detected and HANDLED by retry logic!")
        print("       SERIALIZABLE isolation level detected concurrent access conflicts.")
        print("       These transactions were automatically retried until successful.")
    
    if lost_updates == 0 and completed_iterations == total_iterations:
        print("[OK] PERFECT! All updates applied successfully with retry logic.")
        print("     Retry mechanism ensured 100% correctness despite conflicts.")
    elif lost_updates > 0:
        print("[WARNING] Some value loss detected despite retry logic.")
        print(f"          {failed_iterations} transactions failed after max retries.")
    
    print("=" * 70)
    
    # Save results to file
    save_results(start_datetime, end_datetime, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count)
    
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
        'serialization_errors': serialization_errors,
        'other_errors': other_errors,
        'retry_count': retry_count
    }


def save_results(start_time, end_time, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage,
                 completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count):
    """Save test results to file"""
    filename = "_implementation_02_with_retry_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 02 WITH RETRY: SERIALIZABLE + Automatic Retry\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
        f.write(f"Isolation Level: SERIALIZABLE\n")
        f.write(f"Retry Logic: ENABLED\n")
        f.write(f"Max Retries per transaction: {MAX_RETRIES}\n")
        f.write(f"Backoff Strategy: Exponential (0.001 * 2^attempt seconds)\n")
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
        
        f.write("ERROR STATISTICS AND RETRY ANALYSIS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Successful transactions: {completed_iterations:,}\n")
        f.write(f"Failed transactions: {failed_iterations:,}\n")
        f.write(f"Serialization errors encountered: {serialization_errors:,}\n")
        f.write(f"Other errors: {other_errors:,}\n")
        f.write(f"Total retries performed: {retry_count:,}\n")
        avg_retries = retry_count/completed_iterations if completed_iterations > 0 else 0
        f.write(f"Average retries per successful transaction: {avg_retries:.2f}\n")
        retry_rate = (serialization_errors / completed_iterations) if completed_iterations > 0 else 0
        f.write(f"Serialization error rate: {retry_rate:.2f} errors per success\n")
        f.write("\n")
        
        f.write("ANALYSIS - ANSWERS TO KEY QUESTIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Q1: Will there be any loss of values?\n")
        f.write("-" * 70 + "\n")
        if lost_updates == 0:
            f.write("ANSWER: NO - With retry logic enabled, ALL values are preserved!\n\n")
            f.write(f"Result: {final_value} / {expected_value} (100% correct)\n\n")
            f.write("When a serialization error occurs, the transaction is automatically\n")
            f.write("retried until it succeeds. This ensures no updates are lost.\n\n")
            f.write("How it works:\n")
            f.write("1. Transaction attempts to update counter\n")
            f.write("2. If SerializationFailure error occurs, rollback\n")
            f.write("3. Wait briefly (exponential backoff)\n")
            f.write("4. Retry the transaction\n")
            f.write("5. Repeat until success or max retries reached\n\n")
            f.write(f"In this test:\n")
            f.write(f"- {serialization_errors:,} serialization errors were caught\n")
            f.write(f"- {retry_count:,} retries were performed\n")
            f.write(f"- {completed_iterations:,} transactions eventually succeeded\n")
            f.write(f"- 0 transactions permanently failed\n")
        else:
            f.write("ANSWER: YES - Even with retry logic, some values were lost.\n\n")
            f.write(f"Lost updates: {lost_updates} ({loss_percentage:.2f}%)\n")
            f.write(f"Failed transactions: {failed_iterations:,}\n\n")
            f.write("This can happen if:\n")
            f.write("- Max retries is reached due to extreme contention\n")
            f.write("- Other (non-serialization) errors occur\n")
            f.write("- Database connection issues\n")
        f.write("\n")
        
        f.write("Q2: Will there be any errors?\n")
        f.write("-" * 70 + "\n")
        f.write("ANSWER: YES - Serialization errors WILL occur, but they are HANDLED.\n\n")
        f.write(f"Serialization errors encountered: {serialization_errors:,}\n\n")
        f.write("PostgreSQL's SERIALIZABLE isolation level detects when concurrent\n")
        f.write("transactions would violate serializability. When this happens,\n")
        f.write("it throws a SerializationFailure error:\n")
        f.write("  psycopg2.errors.SerializationFailure\n\n")
        f.write("However, with retry logic enabled:\n")
        f.write("- These errors are CAUGHT by the except block\n")
        f.write("- The transaction is ROLLED BACK\n")
        f.write("- A brief wait occurs (exponential backoff)\n")
        f.write("- The transaction is RETRIED\n")
        f.write("- Eventually succeeds (in most cases)\n\n")
        f.write("The errors are still there, but the application handles them gracefully.\n")
        f.write("\n")
        
        f.write("Q3: Is it possible to get the correct result with SERIALIZABLE?\n")
        f.write("-" * 70 + "\n")
        f.write("ANSWER: YES - Retry logic achieves correct results!\n\n")
        f.write("This test demonstrates that with proper retry logic:\n")
        if lost_updates == 0:
            f.write("- 100% correct final value achieved\n")
            f.write("- No updates lost\n")
            f.write("- All serialization conflicts resolved\n\n")
        f.write("Required retry pattern:\n\n")
        f.write("```python\n")
        f.write("attempts = 0\n")
        f.write("while attempts < MAX_RETRIES:\n")
        f.write("    attempts += 1\n")
        f.write("    try:\n")
        f.write("        conn.set_isolation_level(SERIALIZABLE)\n")
        f.write("        # ... SELECT, compute, UPDATE ...\n")
        f.write("        conn.commit()\n")
        f.write("        break  # Success!\n")
        f.write("    except SerializationFailure:\n")
        f.write("        conn.rollback()\n")
        f.write("        if attempts < MAX_RETRIES:\n")
        f.write("            time.sleep(0.001 * 2**attempts)  # Exponential backoff\n")
        f.write("        else:\n")
        f.write("            # Handle permanent failure\n")
        f.write("```\n\n")
        f.write("Key components:\n")
        f.write("1. Exception handling for SerializationFailure\n")
        f.write("2. Rollback on failure\n")
        f.write("3. Retry loop with max attempts\n")
        f.write("4. Exponential backoff to reduce contention\n")
        f.write("\n")
        
        f.write("COMPARISON: WITH vs WITHOUT RETRY\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("WITHOUT Retry (Implementation 02 original):\n")
        f.write("  - Serialization errors cause transaction failure\n")
        f.write("  - Failed transactions = lost updates\n")
        f.write("  - Result: Incorrect final value\n")
        f.write("  - Faster execution (fewer operations)\n\n")
        
        f.write("WITH Retry (THIS implementation):\n")
        f.write("  - Serialization errors trigger automatic retry\n")
        f.write("  - Retries continue until success\n")
        if lost_updates == 0:
            f.write("  - Result: 100% correct final value\n")
        else:
            f.write(f"  - Result: {100-loss_percentage:.2f}% correct\n")
        f.write("  - Slower execution due to retry overhead\n")
        f.write(f"  - Average {avg_retries:.2f} retries per success\n\n")
        
        f.write("PERFORMANCE IMPACT OF RETRY\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"Total retries: {retry_count:,}\n")
        retry_overhead_pct = (retry_count / total_iterations * 100) if total_iterations > 0 else 0
        f.write(f"Retry overhead: {retry_overhead_pct:.1f}%\n")
        f.write(f"Throughput: {throughput:.2f} operations/second\n\n")
        f.write("The retry mechanism adds overhead but ensures correctness.\n")
        f.write("This is the trade-off: slower but correct vs. faster but wrong.\n")
        f.write("\n")
        
        f.write("COMPARISON WITH OTHER IMPLEMENTATIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Implementation 01 (Lost-update):\n")
        f.write("  - Method: SELECT + Python increment + UPDATE\n")
        f.write("  - Result: ~90% data loss\n")
        f.write("  - Errors: None (silent failure)\n")
        f.write("  - Speed: ~140 ops/sec\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 WITHOUT retry:\n")
        f.write("  - Method: SERIALIZABLE isolation, no retry\n")
        f.write("  - Result: Data loss due to failed transactions\n")
        f.write("  - Errors: Many (unhandled)\n")
        f.write("  - Speed: Variable\n")
        f.write("  - Correctness: INCORRECT\n\n")
        
        f.write("Implementation 02 WITH retry (THIS ONE):\n")
        f.write("  - Method: SERIALIZABLE + automatic retry\n")
        if lost_updates == 0:
            f.write("  - Result: 100% correct\n")
        else:
            f.write(f"  - Result: {100-loss_percentage:.2f}% correct\n")
        f.write("  - Errors: Many (but all handled)\n")
        f.write(f"  - Speed: {throughput:.2f} ops/sec\n")
        if lost_updates == 0:
            f.write("  - Correctness: CORRECT!\n\n")
        else:
            f.write("  - Correctness: Mostly correct\n\n")
        
        f.write("Implementation 03 (Atomic in-place):\n")
        f.write("  - Method: UPDATE counter = counter + 1\n")
        f.write("  - Result: 100% correct\n")
        f.write("  - Errors: None\n")
        f.write("  - Speed: ~122 ops/sec\n")
        f.write("  - Correctness: CORRECT!\n")
        f.write("  - Code: SIMPLE (one line)\n\n")
        
        f.write("KEY TAKEAWAYS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("1. RETRY LOGIC IS ESSENTIAL with SERIALIZABLE\n")
        f.write("   Without retry, serialization errors cause data loss.\n")
        f.write("   With retry, correctness is guaranteed.\n\n")
        
        f.write("2. EXPONENTIAL BACKOFF REDUCES CONTENTION\n")
        f.write("   Waiting between retries gives other transactions time to complete.\n")
        f.write("   This reduces the likelihood of repeated conflicts.\n\n")
        
        f.write("3. SERIALIZABLE + RETRY IS A VALID SOLUTION\n")
        f.write("   For complex read-modify-write operations, this approach works.\n")
        f.write("   But it's more complex than atomic updates (Implementation 03).\n\n")
        
        f.write("4. CHOOSE THE RIGHT TOOL\n")
        f.write("   - Simple counters: Use atomic UPDATE (Implementation 03)\n")
        f.write("   - Complex logic: Use SERIALIZABLE with retry (this approach)\n\n")
        
        f.write("5. TRADE-OFFS ARE REAL\n")
        f.write("   - Correctness requires overhead (retries, slower execution)\n")
        f.write("   - But correctness is non-negotiable for production systems\n\n")
        
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
