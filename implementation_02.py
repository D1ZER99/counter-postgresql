"""
Implementation 02: SERIALIZABLE Transaction Isolation Level

This implementation uses PostgreSQL's SERIALIZABLE isolation level, which is the
strictest isolation level. It detects serialization conflicts and throws errors
when concurrent transactions would violate serializability.

Key differences from Implementation 01:
- Uses SERIALIZABLE isolation level
- Detects and reports serialization errors
- Can optionally implement retry logic

Expected behavior:
- WITHOUT retry: Many serialization errors, value loss
- WITH retry: Correct final value, but slower execution
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
USE_RETRY_LOGIC = False  # Set to True to enable automatic retry on serialization errors
MAX_RETRIES = 5

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
    Worker thread that performs updates with SERIALIZABLE isolation level
    """
    global completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count, progress_bar
    
    # Each thread creates its own connection
    conn = psycopg2.connect(**DB_CONFIG)
    
    for i in range(ITERATIONS_PER_THREAD):
        success = False
        attempts = 0
        
        while not success and attempts < (MAX_RETRIES if USE_RETRY_LOGIC else 1):
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
                
                if USE_RETRY_LOGIC and attempts < MAX_RETRIES:
                    # Retry the transaction
                    time.sleep(0.001 * attempts)  # Exponential backoff
                else:
                    # Give up
                    success = True  # Exit retry loop
                    with progress_lock:
                        failed_iterations += 1
                        
            except Exception as e:
                conn.rollback()
                with progress_lock:
                    other_errors += 1
                    failed_iterations += 1
                success = True  # Exit retry loop
                
            finally:
                cursor.close()
        
        # Update progress
        with progress_lock:
            if progress_bar:
                progress_bar.update(1)
    
    conn.close()


def run_test():
    """Run the SERIALIZABLE isolation level test"""
    global progress_bar, completed_iterations, failed_iterations, serialization_errors, other_errors, retry_count
    
    print("=" * 70)
    print("Implementation 02: SERIALIZABLE Transaction Isolation Level")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print(f"  - Isolation Level: SERIALIZABLE")
    print(f"  - Retry Logic: {'ENABLED' if USE_RETRY_LOGIC else 'DISABLED'}")
    if USE_RETRY_LOGIC:
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
    if USE_RETRY_LOGIC:
        print(f"Total retries: {retry_count:,}")
        print(f"Average retries per success: {retry_count/completed_iterations:.2f}" if completed_iterations > 0 else "N/A")
    print()
    
    if serialization_errors > 0:
        print("[INFO] Serialization errors detected!")
        print("       SERIALIZABLE isolation level detected concurrent access conflicts.")
        print("       These transactions were rolled back to maintain consistency.")
        if not USE_RETRY_LOGIC:
            print("       Without retry logic, these rolled-back transactions are lost.")
            print("       This causes the final counter to be less than expected.")
    
    if lost_updates == 0 and completed_iterations == total_iterations:
        print("[OK] Perfect! All updates applied successfully.")
    elif lost_updates > 0:
        print("[WARNING] Value loss detected due to failed transactions.")
    
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
    filename = "_implementation_02_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 02: SERIALIZABLE Transaction Isolation Level\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
        f.write(f"Isolation Level: SERIALIZABLE\n")
        f.write(f"Retry Logic: {'ENABLED' if USE_RETRY_LOGIC else 'DISABLED'}\n")
        if USE_RETRY_LOGIC:
            f.write(f"Max Retries: {MAX_RETRIES}\n")
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
        
        f.write("ERROR STATISTICS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Successful transactions: {completed_iterations:,}\n")
        f.write(f"Failed transactions: {failed_iterations:,}\n")
        f.write(f"Serialization errors: {serialization_errors:,}\n")
        f.write(f"Other errors: {other_errors:,}\n")
        if USE_RETRY_LOGIC:
            f.write(f"Total retries: {retry_count:,}\n")
            avg_retries = retry_count/completed_iterations if completed_iterations > 0 else 0
            f.write(f"Average retries per success: {avg_retries:.2f}\n")
        f.write("\n")
        
        f.write("ANALYSIS - ANSWERS TO KEY QUESTIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Q1: Will there be any loss of values?\n")
        f.write("-" * 70 + "\n")
        if USE_RETRY_LOGIC:
            if lost_updates == 0:
                f.write("ANSWER: NO - With retry logic enabled, all values are preserved.\n")
                f.write("When a serialization error occurs, the transaction is automatically\n")
                f.write("retried until it succeeds. This ensures no updates are lost.\n")
            else:
                f.write("ANSWER: YES - Even with retry logic, some values were lost.\n")
                f.write("This can happen if max retries is reached or other errors occur.\n")
        else:
            if lost_updates > 0:
                f.write("ANSWER: YES - Without retry logic, there IS value loss.\n")
                f.write(f"Lost updates: {lost_updates} ({loss_percentage:.2f}%)\n")
                f.write(f"Serialization errors: {serialization_errors:,}\n\n")
                f.write("When SERIALIZABLE isolation detects a conflict, it throws a\n")
                f.write("SerializationFailure error and rolls back the transaction.\n")
                f.write("Without retry logic, these rolled-back transactions are lost,\n")
                f.write("resulting in a lower final counter value.\n")
            else:
                f.write("ANSWER: NO - Surprisingly, no value loss occurred.\n")
                f.write("This can happen if there were few conflicts or lucky timing.\n")
        f.write("\n")
        
        f.write("Q2: Will there be any errors?\n")
        f.write("-" * 70 + "\n")
        if serialization_errors > 0:
            f.write("ANSWER: YES - Serialization errors WILL occur.\n")
            f.write(f"Serialization errors encountered: {serialization_errors:,}\n\n")
            f.write("PostgreSQL's SERIALIZABLE isolation level detects when concurrent\n")
            f.write("transactions would violate serializability. When this happens,\n")
            f.write("it throws a SerializationFailure error:\n")
            f.write("  psycopg2.errors.SerializationFailure\n\n")
            f.write("This is by design - SERIALIZABLE prevents anomalies by detecting\n")
            f.write("conflicts and forcing one transaction to retry.\n")
        else:
            f.write("ANSWER: NO - No serialization errors occurred.\n")
            f.write("This is unusual and may indicate low contention or lucky timing.\n")
        f.write("\n")
        
        f.write("Q3: Can we modify the code to always get the correct result?\n")
        f.write("-" * 70 + "\n")
        f.write("ANSWER: YES - By implementing retry logic!\n\n")
        if USE_RETRY_LOGIC:
            f.write("This test RAN WITH retry logic enabled.\n")
            f.write(f"Result: {completed_iterations:,} successful, {failed_iterations:,} failed\n")
            f.write(f"Total retries needed: {retry_count:,}\n\n")
        else:
            f.write("This test ran WITHOUT retry logic.\n")
            f.write("To get correct results, the code should:\n\n")
        f.write("1. Catch SerializationFailure exceptions\n")
        f.write("2. Roll back the failed transaction\n")
        f.write("3. Retry the entire transaction\n")
        f.write("4. Use exponential backoff to reduce contention\n\n")
        f.write("Example retry pattern:\n")
        f.write("  attempts = 0\n")
        f.write("  while attempts < MAX_RETRIES:\n")
        f.write("    try:\n")
        f.write("      # ... transaction code ...\n")
        f.write("      conn.commit()\n")
        f.write("      break  # Success!\n")
        f.write("    except SerializationFailure:\n")
        f.write("      conn.rollback()\n")
        f.write("      attempts += 1\n")
        f.write("      time.sleep(0.001 * attempts)  # Exponential backoff\n\n")
        f.write("With proper retry logic, SERIALIZABLE isolation level provides:\n")
        f.write("- Complete consistency (no lost updates)\n")
        f.write("- Serializability guarantee\n")
        f.write("- Protection against all anomalies\n")
        f.write("\n")
        
        f.write("COMPARISON WITH IMPLEMENTATION 01\n")
        f.write("-" * 70 + "\n")
        f.write("Implementation 01 (READ COMMITTED, no locking):\n")
        f.write("  - Silent data loss (~90% lost updates)\n")
        f.write("  - No errors thrown\n")
        f.write("  - Faster execution\n")
        f.write("  - Incorrect results\n\n")
        f.write("Implementation 02 (SERIALIZABLE):\n")
        f.write("  - Detects conflicts\n")
        f.write("  - Throws serialization errors\n")
        if USE_RETRY_LOGIC:
            f.write("  - With retry: Correct results\n")
            f.write("  - Slower due to retries\n")
        else:
            f.write("  - Without retry: Data loss (but errors are visible)\n")
            f.write("  - Faster than with retry\n")
        f.write("\n")
        
        f.write("KEY TAKEAWAYS\n")
        f.write("-" * 70 + "\n")
        f.write("1. SERIALIZABLE isolation level DETECTS conflicts that would be\n")
        f.write("   silently lost with lower isolation levels.\n\n")
        f.write("2. Applications MUST implement retry logic to handle serialization\n")
        f.write("   failures and achieve correct results.\n\n")
        f.write("3. SERIALIZABLE provides the strongest consistency guarantees but\n")
        f.write("   at the cost of increased errors and need for retry logic.\n\n")
        f.write("4. The errors are a FEATURE, not a bug - they prevent data anomalies.\n\n")
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
