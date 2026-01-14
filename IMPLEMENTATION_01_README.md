# Implementation 01: Lost-Update (Race Condition Demo)

## Overview

This implementation demonstrates the **lost-update problem** - a classic race condition that occurs when multiple threads access shared data without proper synchronization.

## The Problem

When multiple threads perform a read-modify-write cycle without locking:

```
Thread A: SELECT counter (gets 100)
Thread B: SELECT counter (gets 100)
Thread A: UPDATE counter = 101
Thread B: UPDATE counter = 101  ← Lost update! Should be 102
```

Result: Only 1 increment is applied instead of 2.

## How It Works

### Each Thread Executes:
```python
for i in range(10_000):
    # 1. SELECT counter value
    counter = cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1").fetchone()[0]
    
    # 2. Increment in Python (race condition here!)
    counter = counter + 1
    
    # 3. UPDATE with new value
    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
    
    # 4. COMMIT (separate transaction)
    conn.commit()
```

### Configuration:
- **Threads**: 10
- **Iterations per thread**: 10,000
- **Total operations**: 100,000
- **Expected final value**: 100,000
- **Actual final value**: Much less (due to lost updates)

## Running the Test

### Prerequisites

1. PostgreSQL container must be running:
```bash
docker-compose up -d
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Ensure the `user_counter` table exists with user_id = 1

### Execute the Test

```bash
python implementation_01.py
```

### Expected Output

```
======================================================================
Implementation 01: Lost-update (Race Condition Demo)
======================================================================

Configuration:
  - Number of threads: 10
  - Iterations per thread: 10,000
  - Total expected increments: 100,000
  - User ID: 1

✓ Counter reset to 0 for user_id = 1
Initial counter value: 0

Processing: 100%|████████████████████| 100000/100000 [00:45<00:00, 2213.45ops/s]

======================================================================
RESULTS
======================================================================
Start time: 2026-01-13 10:30:15
End time: 2026-01-13 10:30:60
Execution time: 45.18 seconds
Throughput: 2213.45 operations/second

Initial counter value: 0
Final counter value: 12,543
Expected counter value: 100,000
Lost updates: 87,457 (87.46%)

⚠ WARNING: Lost updates detected! This demonstrates the race condition.
   Multiple threads read the same value, increment it, and write back,
   causing some updates to be lost.
======================================================================

✓ Results saved to: _implementation_01_results.txt
```

## Results File

The test automatically saves detailed results to `_implementation_01_results.txt`, including:
- Test configuration
- Execution timing
- Counter values (initial, final, expected)
- Lost update analysis
- Explanation of the race condition

## Why This Happens

### The Race Condition Window

The problem occurs in the gap between SELECT and UPDATE:

```
Time | Thread 1              | Thread 2              | Counter in DB
-----|----------------------|----------------------|---------------
t1   | SELECT (counter=100) |                      | 100
t2   |                      | SELECT (counter=100) | 100
t3   | counter = 101        |                      | 100
t4   |                      | counter = 101        | 100
t5   | UPDATE counter=101   |                      | 101
t6   | COMMIT               |                      | 101
t7   |                      | UPDATE counter=101   | 101
t8   |                      | COMMIT               | 101 ← Lost update!
```

**Result**: Two increments resulted in only one actual increment.

## Key Characteristics

✗ **No Synchronization**: No locks or version checking
✗ **Read-Modify-Write Pattern**: Classic race condition scenario
✗ **Separate Transactions**: Each operation commits independently
✗ **Data Loss**: Updates are silently lost (no errors thrown)

## Expected Loss Rate

The loss rate depends on:
- Number of concurrent threads (more = higher loss)
- Database latency (higher latency = more overlap = more loss)
- System load

Typical results: **70-95% loss rate**

## Next Steps

To fix this problem, subsequent implementations will use:
- **Implementation 02**: Row-level locking (SELECT FOR UPDATE)
- **Implementation 03**: Optimistic locking with version field
- **Implementation 04**: Atomic UPDATE operations
- **Implementation 05**: Advisory locks

## Learning Points

1. **Race conditions are real** and can cause significant data loss
2. **They're silent** - no errors are thrown
3. **Testing reveals them** - single-threaded code works fine
4. **Synchronization is essential** for concurrent data access
5. **Database features help** - use locks, transactions, and constraints

## Documentation Reference

- [Psycopg2 Usage Guide](https://www.psycopg.org/docs/usage.html)
- [PostgreSQL Concurrency Control](https://www.postgresql.org/docs/current/mvcc.html)
