"""
Implementation 01: Lost-update (demonstrates race condition)

This implementation intentionally has a flaw - it will lose updates due to race conditions.
Multiple threads read the counter value, increment it, and write it back without any locking,
causing concurrent updates to be lost.

Each thread:
1. SELECT counter value
2. Increment in Python
3. UPDATE with new value
4. COMMIT (separate transaction per update)

Expected behavior: Counter should be 100,000 (10 threads × 10,000 iterations)
Actual behavior: Counter will be much less due to lost updates
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

# Global progress tracking
progress_lock = threading.Lock()
total_iterations = NUM_THREADS * ITERATIONS_PER_THREAD
completed_iterations = 0
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
    Worker thread that performs the lost-update pattern
    Each iteration uses a separate transaction
    """
    global completed_iterations, progress_bar
    
    # Each thread creates its own connection
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # Manual transaction control
    
    for i in range(ITERATIONS_PER_THREAD):
        cursor = conn.cursor()
        
        try:
            # Step 1: SELECT counter value
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
            result = cursor.fetchone()
            counter = result[0] if result else 0
            
            # Step 2: Increment in Python (this is where the race condition happens)
            counter = counter + 1
            
            # Step 3: UPDATE with new value
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, USER_ID))
            
            # Step 4: COMMIT (separate transaction for each record)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            print(f"\nError in thread {thread_id}, iteration {i}: {e}")
        finally:
            cursor.close()
        
        # Update progress
        with progress_lock:
            completed_iterations += 1
            if progress_bar:
                progress_bar.update(1)
    
    conn.close()


def run_test():
    """Run the lost-update test with multiple threads"""
    global progress_bar, completed_iterations
    
    print("=" * 70)
    print("Implementation 01: Lost-update (Race Condition Demo)")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  - Number of threads: {NUM_THREADS}")
    print(f"  - Iterations per thread: {ITERATIONS_PER_THREAD:,}")
    print(f"  - Total expected increments: {total_iterations:,}")
    print(f"  - User ID: {USER_ID}")
    print()
    
    # Reset counter
    reset_counter()
    initial_value = get_counter_value()
    print(f"Initial counter value: {initial_value}")
    print()
    
    # Create progress bar
    completed_iterations = 0
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
    
    if lost_updates > 0:
        print("[WARNING] Lost updates detected! This demonstrates the race condition.")
        print("          Multiple threads read the same value, increment it, and write back,")
        print("          causing some updates to be lost.")
    else:
        print("[OK] No lost updates detected (this is rare with this implementation)")
    
    print("=" * 70)
    
    # Save results to file
    save_results(start_datetime, end_datetime, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage)
    
    return {
        'elapsed_time': elapsed_time,
        'throughput': throughput,
        'initial_value': initial_value,
        'final_value': final_value,
        'expected_value': expected_value,
        'lost_updates': lost_updates,
        'loss_percentage': loss_percentage
    }


def save_results(start_time, end_time, elapsed_time, throughput,
                 initial_value, final_value, expected_value, lost_updates, loss_percentage):
    """Save test results to file"""
    filename = "_implementation_01_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 01: Lost-update (Race Condition Demo)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Number of threads: {NUM_THREADS}\n")
        f.write(f"Iterations per thread: {ITERATIONS_PER_THREAD:,}\n")
        f.write(f"Total operations: {total_iterations:,}\n")
        f.write(f"User ID: {USER_ID}\n")
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
        f.write("\n")
        
        f.write("RACE CONDITION ANALYSIS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Lost updates: {lost_updates}\n")
        f.write(f"Loss percentage: {loss_percentage:.2f}%\n")
        f.write(f"Successfully applied updates: {final_value - initial_value}\n")
        f.write(f"Success rate: {100 - loss_percentage:.2f}%\n")
        f.write("\n")
        
        f.write("EXPLANATION\n")
        f.write("-" * 70 + "\n")
        f.write("This implementation demonstrates the 'lost-update' problem:\n")
        f.write("1. Thread A reads counter = 100\n")
        f.write("2. Thread B reads counter = 100 (same value)\n")
        f.write("3. Thread A increments to 101 and writes it\n")
        f.write("4. Thread B increments to 101 and writes it (overwriting A's update)\n")
        f.write("5. Result: Only 1 increment applied instead of 2\n")
        f.write("\n")
        f.write("This is a classic race condition that occurs when:\n")
        f.write("- Multiple threads access shared data concurrently\n")
        f.write("- No synchronization mechanism is used\n")
        f.write("- The read-modify-write cycle is not atomic\n")
        f.write("\n")
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
