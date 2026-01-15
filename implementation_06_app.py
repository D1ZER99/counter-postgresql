"""
Web Counter Application with PostgreSQL Backend
Uses atomic in-place updates (Implementation 03 pattern) for thread-safe counter
"""
from flask import Flask, jsonify
import psycopg2
from psycopg2 import pool
import atexit

app = Flask(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'counter_db',
    'user': 'counter_user',
    'password': 'counter_password'
}

USER_ID = 1  # Using user_id = 1 for the counter

# Connection pool for better performance and reliability
# Min connections: 5, Max connections: 20
# This reduces connection overhead and handles concurrent requests better
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=5,
        maxconn=20,
        **DB_CONFIG
    )
except Exception as e:
    print(f"Warning: Could not create connection pool: {e}")
    print("Falling back to per-request connections")
    connection_pool = None


def get_db_connection():
    """Get a database connection from pool or create new one"""
    if connection_pool:
        try:
            return connection_pool.getconn()
        except Exception:
            # Fallback to new connection if pool fails
            return psycopg2.connect(**DB_CONFIG)
    else:
        return psycopg2.connect(**DB_CONFIG)


def return_db_connection(conn):
    """Return connection to pool"""
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
        except Exception:
            conn.close()


@atexit.register
def close_pool():
    """Close connection pool on exit"""
    if connection_pool:
        connection_pool.closeall()


@app.route('/')
def home():
    """Home page to verify server is running"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
        result = cursor.fetchone()
        current_count = result[0] if result else 0
        cursor.close()
        return_db_connection(conn)
        
        return jsonify({
            'status': 'running',
            'message': 'Web Counter Application with PostgreSQL',
            'storage': 'PostgreSQL with atomic in-place updates',
            'connection_pool': 'enabled' if connection_pool else 'disabled',
            'current_count': current_count,
            'endpoints': {
                '/inc': 'Increment counter (GET/POST)',
                '/count': 'Get current count (GET)',
                '/reset': 'Reset counter to 0 (POST)'
            }
        })
    except Exception as e:
        if conn:
            return_db_connection(conn)
        return jsonify({'error': str(e)}), 500


@app.route('/inc', methods=['GET', 'POST'])
def increment():
    """Increment the counter by 1 using atomic UPDATE"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Atomic in-place update (Implementation 03 pattern)
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (USER_ID,))
        conn.commit()
        
        cursor.close()
        return_db_connection(conn)
        
        return '', 204  # No content response for faster processing
    except Exception as e:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        return jsonify({'error': str(e)}), 500


@app.route('/count', methods=['GET'])
def get_count():
    """Get the current counter value"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
        result = cursor.fetchone()
        current_count = result[0] if result else 0
        cursor.close()
        return_db_connection(conn)
        
        return jsonify({'count': current_count})
    except Exception as e:
        if conn:
            return_db_connection(conn)
        return jsonify({'error': str(e)}), 500


@app.route('/reset', methods=['POST'])
def reset():
    """Reset the counter to 0"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE user_counter SET counter = 0 WHERE user_id = %s", (USER_ID,))
        conn.commit()
        cursor.close()
        return_db_connection(conn)
        
        return '', 204
    except Exception as e:
        if conn:
            conn.rollback()
            return_db_connection(conn)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("Starting Web Counter Application with PostgreSQL")
    print("=" * 60)
    print("URL: http://127.0.0.1:8080")
    print("=" * 60)
    
    try:
        from waitress import serve
        print("Server: Waitress (Production WSGI Server)")
        print("Threads: 100")
        print("\nServer is ready to accept connections!")
        print("Test: Open http://127.0.0.1:8080 in your browser")
        print("Press Ctrl+C to stop\n")
        
        # Serve with waitress
        serve(
            app,
            host='0.0.0.0',
            port=8080,
            threads=100,
            channel_timeout=30
        )
    except ImportError:
        print("Server: Flask (Development - Install waitress for better performance)")
        print("\nServer is ready to accept connections!")
        print("Test: Open http://127.0.0.1:8080 in your browser")
        print("Press Ctrl+C to stop\n")
        app.run(host='0.0.0.0', port=8080, threaded=True, debug=False)
    except KeyboardInterrupt:
        print("\n\nServer stopped by user")
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
