"""
Test script to verify PostgreSQL connection and table setup
"""
import psycopg2
from psycopg2 import sql

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'counter_db',
    'user': 'counter_user',
    'password': 'counter_password'
}

def test_connection():
    """Test basic database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Successfully connected to PostgreSQL database")
        
        cursor = conn.cursor()
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✓ PostgreSQL version: {version[0][:50]}...")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def test_table():
    """Test if user_counter table exists and has correct structure"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'user_counter'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print("✗ Table 'user_counter' does not exist")
            return False
        
        print("✓ Table 'user_counter' exists")
        
        # Check table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'user_counter'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\n✓ Table structure:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]} (nullable: {col[2]})")
        
        # Check for primary key
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'user_counter'::regclass AND i.indisprimary;
        """)
        
        pk = cursor.fetchone()
        if pk:
            print(f"\n✓ Primary key: {pk[0]}")
        
        # Count rows
        cursor.execute("SELECT COUNT(*) FROM user_counter;")
        count = cursor.fetchone()[0]
        print(f"\n✓ Current row count: {count}")
        
        # Show sample data
        cursor.execute("SELECT * FROM user_counter LIMIT 5;")
        rows = cursor.fetchall()
        if rows:
            print("\n✓ Sample data:")
            print("  user_id | counter | version")
            print("  --------|---------|--------")
            for row in rows:
                print(f"  {row[0]:7} | {row[1]:7} | {row[2]:7}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Table check failed: {e}")
        return False

def test_operations():
    """Test basic CRUD operations"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\n--- Testing Operations ---")
        
        # Test INSERT
        test_user_id = 9999
        cursor.execute("""
            INSERT INTO user_counter (user_id, counter, version) 
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE 
            SET counter = 0, version = 0;
        """, (test_user_id, 0, 0))
        conn.commit()
        print(f"✓ INSERT test successful (user_id: {test_user_id})")
        
        # Test UPDATE (increment counter)
        cursor.execute("""
            UPDATE user_counter 
            SET counter = counter + 1, version = version + 1
            WHERE user_id = %s
            RETURNING counter, version;
        """, (test_user_id,))
        result = cursor.fetchone()
        print(f"✓ UPDATE test successful (counter: {result[0]}, version: {result[1]})")
        
        # Test SELECT
        cursor.execute("""
            SELECT user_id, counter, version 
            FROM user_counter 
            WHERE user_id = %s;
        """, (test_user_id,))
        row = cursor.fetchone()
        print(f"✓ SELECT test successful (user_id: {row[0]}, counter: {row[1]}, version: {row[2]})")
        
        # Test DELETE (cleanup)
        cursor.execute("DELETE FROM user_counter WHERE user_id = %s;", (test_user_id,))
        conn.commit()
        print(f"✓ DELETE test successful (cleaned up test data)")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Operations test failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("PostgreSQL Counter Database - Connection Test")
    print("=" * 60)
    print()
    
    if not test_connection():
        print("\n⚠ Please ensure PostgreSQL Docker container is running:")
        print("  docker-compose up -d")
        return
    
    print()
    if not test_table():
        return
    
    print()
    if not test_operations():
        return
    
    print()
    print("=" * 60)
    print("✓ All tests passed! Database is ready for Part 02.")
    print("=" * 60)

if __name__ == "__main__":
    main()
