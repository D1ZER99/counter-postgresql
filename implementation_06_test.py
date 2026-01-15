"""
Test Script for Implementation 06: PostgreSQL Web Counter
Tests Scenario 4: 10 clients × 10K requests = 100K total
Saves results to _implementation_06_results.txt
"""
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://127.0.0.1:8080"  # Use IP instead of localhost for speed
REQUESTS_PER_CLIENT = 10000
NUM_CLIENTS = 10
TOTAL_REQUESTS = NUM_CLIENTS * REQUESTS_PER_CLIENT


def make_request(session, url):
    """Make a single HTTP request using a session"""
    try:
        response = session.get(url, timeout=30)
        return response.status_code == 204 or response.status_code == 200
    except requests.exceptions.Timeout:
        return False
    except Exception as e:
        print(f"\nRequest failed: {e}")
        return False


def make_requests_parallel(base_url, num_requests, num_clients):
    """
    Make requests in parallel using multiple clients
    Each client makes (num_requests) requests
    Returns: (elapsed_time, successful_requests, failed_requests)
    """
    url = f"{base_url}/inc"
    completed_count = 0
    successful_count = 0
    failed_count = 0
    lock = __import__('threading').Lock()
    MAX_RETRIES = 3
    
    start_time = time.time()
    
    def client_worker(client_id, num_reqs):
        """Each client uses its own session"""
        nonlocal completed_count, successful_count, failed_count
        with requests.Session() as session:
            session.headers.update({'Connection': 'keep-alive'})
            
            for i in range(num_reqs):
                # Retry logic for failed requests
                success = False
                for attempt in range(MAX_RETRIES):
                    if make_request(session, url):
                        success = True
                        break
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(0.01 * (attempt + 1))  # Brief backoff
                
                with lock:
                    if success:
                        successful_count += 1
                    else:
                        failed_count += 1
                    completed_count += 1
                    
                    if completed_count % 1000 == 0:
                        percentage = (completed_count / TOTAL_REQUESTS) * 100
                        print(f"Progress: {completed_count}/{TOTAL_REQUESTS} ({percentage:.1f}%) - Success: {successful_count}, Failed: {failed_count}", end='\r')
    
    # Use ThreadPoolExecutor to simulate multiple clients
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(client_worker, i, num_requests) for i in range(num_clients)]
        
        # Wait for all clients to complete
        for future in as_completed(futures):
            future.result()
    
    end_time = time.time()
    return end_time - start_time, successful_count, failed_count


def get_count(base_url):
    """Get the current counter value"""
    try:
        response = requests.get(f"{base_url}/count", timeout=10)
        if response.status_code == 200:
            return response.json()['count']
    except Exception as e:
        print(f"Failed to get count: {e}")
    return None


def reset_counter(base_url):
    """Reset the counter to 0"""
    try:
        response = requests.post(f"{base_url}/reset", timeout=10)
        return response.status_code == 204
    except Exception as e:
        print(f"Failed to reset counter: {e}")
        return False


def save_results(start_time, end_time, elapsed_time, throughput,
                 expected_value, final_value, lost_updates, loss_percentage,
                 successful_requests, failed_requests):
    """Save test results to file"""
    filename = "_implementation_06_results.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("Implementation 06: PostgreSQL with Atomic In-Place Updates (Web API)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("TEST CONFIGURATION\n")
        f.write("-" * 70 + "\n")
        f.write(f"Storage: PostgreSQL with atomic UPDATE counter = counter + 1\n")
        f.write(f"Clients: {NUM_CLIENTS} concurrent clients\n")
        f.write(f"Requests per client: {REQUESTS_PER_CLIENT:,}\n")
        f.write(f"Total requests: {TOTAL_REQUESTS:,}\n")
        f.write(f"Server: Waitress (100 threads)\n")
        f.write(f"Database: counter_db on localhost:5432\n")
        f.write(f"User ID: 1\n")
        f.write("\n")
        
        f.write("EXECUTION DETAILS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
        f.write(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
        f.write(f"Execution time: {elapsed_time:.2f} seconds\n")
        f.write(f"Throughput: {throughput:.2f} requests/second\n")
        f.write("\n")
        
        f.write("RESULTS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total HTTP requests sent: {TOTAL_REQUESTS:,}\n")
        f.write(f"Successful HTTP requests: {successful_requests:,}\n")
        f.write(f"Failed HTTP requests: {failed_requests:,}\n")
        f.write(f"Expected counter value: {expected_value:,}\n")
        f.write(f"Final counter value: {final_value:,}\n")
        f.write(f"Lost updates: {lost_updates}\n")
        f.write(f"Loss percentage: {loss_percentage:.2f}%\n")
        success_rate = (100 - loss_percentage) if lost_updates >= 0 else 100
        f.write(f"Success rate: {success_rate:.2f}%\n")
        f.write("\n")
        
        f.write("ANALYSIS OF LOST UPDATES\n")
        f.write("-" * 70 + "\n")
        if failed_requests > 0:
            f.write(f"HTTP Request Failures: {failed_requests:,} requests failed to reach the server\n")
            f.write("Possible causes:\n")
            f.write("  - Connection timeouts (30 second timeout)\n")
            f.write("  - Server overload (too many concurrent connections)\n")
            f.write("  - Database connection pool exhaustion\n")
            f.write("  - Network stack limitations\n\n")
        
        if lost_updates > failed_requests:
            db_failures = lost_updates - failed_requests
            f.write(f"Database Operation Failures: {db_failures:,} requests reached server but failed in database\n")
            f.write("Possible causes:\n")
            f.write("  - Database connection failures\n")
            f.write("  - Transaction rollbacks\n")
            f.write("  - Database connection pool exhaustion\n")
            f.write("  - PostgreSQL max_connections limit\n\n")
        
        if lost_updates == 0:
            f.write("✓ All requests successfully processed!\n")
            f.write("✓ No lost updates detected\n\n")
        f.write("\n")
        
        f.write("COMPARISON WITH OTHER IMPLEMENTATIONS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Implementation 03 (PostgreSQL direct):\n")
        f.write("  - Method: Direct Python connection, atomic UPDATE\n")
        f.write("  - Throughput: ~122 ops/sec\n")
        f.write("  - No HTTP overhead\n")
        f.write("  - Correctness: 100% correct\n\n")
        
        f.write("Implementation 06 (PostgreSQL via HTTP) - THIS ONE:\n")
        f.write(f"  - Method: Flask API + PostgreSQL, atomic UPDATE\n")
        f.write(f"  - Throughput: {throughput:.2f} req/sec\n")
        f.write("  - HTTP overhead: Flask routing, request/response processing\n")
        if lost_updates == 0:
            f.write("  - Correctness: 100% correct\n\n")
        else:
            f.write(f"  - Correctness: {success_rate:.2f}%\n\n")
        
        if throughput > 0:
            overhead_factor = 122 / throughput if throughput > 0 else 0
            f.write(f"HTTP Overhead Analysis:\n")
            f.write(f"  - Direct PostgreSQL: ~122 ops/sec\n")
            f.write(f"  - Via HTTP API: {throughput:.2f} req/sec\n")
            f.write(f"  - Overhead factor: {overhead_factor:.2f}x slower\n")
            f.write(f"  - Overhead percentage: {(1 - throughput/122)*100:.1f}%\n\n")
        
        f.write("web-counter Part 1 (RAM-based):\n")
        f.write("  - Method: In-memory counter with threading.Lock()\n")
        f.write("  - Throughput: ~1000+ req/sec (very fast)\n")
        f.write("  - Storage: RAM (not persistent)\n")
        f.write("  - Correctness: 100% correct\n\n")
        
        f.write("ANALYSIS\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("Performance Bottlenecks:\n")
        f.write("-" * 70 + "\n")
        f.write("1. HTTP REQUEST/RESPONSE OVERHEAD\n")
        f.write("   - TCP connection establishment\n")
        f.write("   - HTTP headers parsing\n")
        f.write("   - Flask routing and middleware\n")
        f.write("   - Response serialization\n\n")
        
        f.write("2. DATABASE CONNECTION OVERHEAD\n")
        f.write("   - Creating new connection per request\n")
        f.write("   - Connection authentication\n")
        f.write("   - Connection cleanup\n")
        f.write("   (Could be optimized with connection pooling)\n\n")
        
        f.write("3. NETWORK STACK (even on localhost)\n")
        f.write("   - Kernel network processing\n")
        f.write("   - Socket I/O\n")
        f.write("   - Context switching\n\n")
        
        f.write("4. DATABASE OPERATION\n")
        f.write("   - SQL parsing and execution\n")
        f.write("   - Row-level locking\n")
        f.write("   - Transaction commit\n")
        f.write("   - Disk I/O (if not in cache)\n\n")
        
        f.write("Why This Implementation is Correct:\n")
        f.write("-" * 70 + "\n")
        if lost_updates == 0:
            f.write("✓ All {:,} requests processed correctly\n".format(TOTAL_REQUESTS))
            f.write("✓ No lost updates despite high concurrency\n")
            f.write("✓ Atomic UPDATE ensures consistency\n")
            f.write("✓ Database handles concurrency automatically\n")
        else:
            f.write("⚠ Some updates were lost ({:,} out of {:,})\n".format(lost_updates, TOTAL_REQUESTS))
        f.write("\n")
        
        f.write("Key Advantages:\n")
        f.write("1. Production-ready architecture (HTTP API + Database)\n")
        f.write("2. Persistent storage (data survives server restarts)\n")
        f.write("3. No application-level locking needed\n")
        f.write("4. Database handles all concurrency\n")
        f.write("5. Scalable (multiple app servers can use same database)\n")
        f.write("\n")
        
        f.write("Trade-offs:\n")
        f.write("- Slower than in-memory (but acceptable for most use cases)\n")
        f.write("- Database becomes bottleneck (single point of contention)\n")
        f.write("- Network latency (even minimal on localhost)\n")
        f.write("\n")
        
        f.write("When to Use This Approach:\n")
        f.write("-" * 70 + "\n")
        f.write("✓ Production web applications\n")
        f.write("✓ When persistence is required\n")
        f.write("✓ When multiple servers need shared state\n")
        f.write("✓ When you need ACID guarantees\n")
        f.write("✓ When counter needs to survive crashes\n")
        f.write("\n")
        f.write("✗ Not ideal for:\n")
        f.write("  - Ultra-high performance requirements (use Redis/Memcached)\n")
        f.write("  - Single-server, non-persistent counters (use RAM)\n")
        f.write("\n")
        
        f.write("=" * 70 + "\n")
    
    print(f"\n[OK] Results saved to: {filename}")


def main():
    print("\n" + "="*70)
    print("Implementation 06: PostgreSQL Web Counter Performance Test")
    print("="*70)
    print(f"\nTest Configuration:")
    print(f"  - Clients: {NUM_CLIENTS}")
    print(f"  - Requests per client: {REQUESTS_PER_CLIENT:,}")
    print(f"  - Total requests: {TOTAL_REQUESTS:,}")
    print(f"  - Storage: PostgreSQL with atomic UPDATE\n")
    print("Make sure the server is running on http://127.0.0.1:8080")
    print("Start the server with: python implementation_06_server.py")
    print("\nPress Enter to continue or Ctrl+C to cancel...")
    try:
        input()
    except:
        pass
    
    # Verify server is running
    print("\nVerifying server connection...")
    try:
        count = get_count(BASE_URL)
        print(f"[OK] Server is running. Current count: {count}")
    except Exception as e:
        print(f"[ERROR] Cannot connect to server at {BASE_URL}")
        print(f"Error: {e}")
        print("Please start the server first with: python implementation_06_server.py")
        return
    
    # Reset counter
    print("\nResetting counter to 0...")
    if reset_counter(BASE_URL):
        print("[OK] Counter reset")
        time.sleep(0.5)  # Brief pause to ensure reset
    else:
        print("[WARNING] Failed to reset counter")
    
    # Run test
    print(f"\n{'='*70}")
    print("Scenario 4: Ten clients, 10K requests each (100K total)")
    print("="*70)
    print("\nRunning requests with 10 concurrent clients...")
    print("This may take several minutes...\n")
    
    start_datetime = datetime.now()
    elapsed_time, successful_requests, failed_requests = make_requests_parallel(BASE_URL, REQUESTS_PER_CLIENT, NUM_CLIENTS)
    end_datetime = datetime.now()
    print()  # New line after progress
    
    # Get final count
    final_value = get_count(BASE_URL)
    expected_value = TOTAL_REQUESTS
    # Lost updates = requests that didn't reach the database
    # This could be due to HTTP failures OR database failures
    lost_updates = expected_value - final_value if final_value else expected_value
    loss_percentage = (lost_updates / expected_value) * 100 if expected_value > 0 else 0
    throughput = successful_requests / elapsed_time if elapsed_time > 0 else 0
    
    # Print results
    print("="*70)
    print("RESULTS")
    print("="*70)
    print(f"Total requests sent: {TOTAL_REQUESTS:,}")
    print(f"Successful HTTP requests: {successful_requests:,}")
    print(f"Failed HTTP requests: {failed_requests:,}")
    print(f"Final counter value: {final_value:,}")
    print(f"Expected counter value: {expected_value:,}")
    print(f"Lost updates: {lost_updates}")
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
    print(f"Successful requests per second: {throughput:.2f}")
    print("="*70)
    
    # Save results
    save_results(start_datetime, end_datetime, elapsed_time, throughput,
                 expected_value, final_value, lost_updates, loss_percentage,
                 successful_requests, failed_requests)
    
    print("\n[OK] Testing Complete!")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[WARNING] Test interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
