"""
Production Server for Web Counter Application with PostgreSQL
Uses Waitress WSGI server (Windows-compatible, production-ready)
"""
import sys
from waitress import serve
from implementation_06_app import app

def main():
    try:
        print("="*60)
        print("Starting Web Counter Application with PostgreSQL")
        print("="*60)
        print("Server: Waitress (Production WSGI Server)")
        print("URL: http://127.0.0.1:8080")
        print("Storage: PostgreSQL with atomic in-place updates")
        print("Threads: 100 (high concurrency support)")
        print("="*60)
        print("\nServer is ready to accept connections...")
        print("Test it: Open http://127.0.0.1:8080 in your browser")
        print("Press Ctrl+C to stop the server\n")
        
        # Serve with waitress - production-ready WSGI server
        # Works great on Windows with high concurrency
        serve(
            app,
            host='0.0.0.0',
            port=8080,
            threads=100,  # Support up to 100 concurrent requests
            channel_timeout=30,
            connection_limit=1000,
            cleanup_interval=10,
            expose_tracebacks=False
        )
    except KeyboardInterrupt:
        print("\n\nServer stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nERROR: Server failed to start!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
