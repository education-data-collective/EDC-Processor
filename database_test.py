#!/usr/bin/env python3
"""
Simple Database Connection Test

This script connects to the test database and checks what data is available.
Simplified version focusing on connection testing and data exploration.
"""

import os
import sys
import subprocess
import time
import socket
import signal
from pathlib import Path

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy")
    sys.exit(1)

# Configuration - update these values as needed
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = './etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# Global variables for cleanup
proxy_process = None

def signal_handler(signum, frame):
    global proxy_process
    print(f"\nReceived signal {signum}. Cleaning up...")
    if proxy_process:
        stop_cloud_sql_proxy(proxy_process)
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def find_free_port():
    """Find a free port for the proxy"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def start_cloud_sql_proxy():
    """Start Cloud SQL Proxy"""
    global proxy_process
    port = find_free_port()
    
    proxy_commands = ['cloud-sql-proxy', 'cloud_sql_proxy']
    proxy_cmd = None
    
    for cmd in proxy_commands:
        try:
            subprocess.run([cmd, '--version'], capture_output=True, check=True)
            proxy_cmd = [
                cmd,
                f'-instances={CLOUD_SQL_CONNECTION_NAME}=tcp:{port}',
                f'-credential_file={SERVICE_ACCOUNT_FILE}',
                '-max_connections=10',  # Reduced for testing
            ]
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    if not proxy_cmd:
        raise Exception("Cloud SQL Proxy not found. Please install it first.")
    
    print(f"Starting Cloud SQL Proxy on port {port}")
    
    proxy_process = subprocess.Popen(proxy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(5)
    
    if proxy_process.poll() is not None:
        _, stderr = proxy_process.communicate()
        raise Exception(f"Cloud SQL Proxy failed to start: {stderr.decode()}")
    
    print("✅ Cloud SQL Proxy started successfully")
    return proxy_process, port

def stop_cloud_sql_proxy(proxy_process):
    """Stop Cloud SQL Proxy"""
    if proxy_process:
        proxy_process.terminate()
        try:
            proxy_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proxy_process.kill()
            proxy_process.wait()
        print("Cloud SQL Proxy stopped")

def test_connection(port):
    """Test database connection"""
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        print(f"Testing connection: postgresql://{DB_USER}:***@localhost:{port}/{DB_NAME}")
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Database connection successful!")
            return True, connection_string, engine
            
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return False, None, None

def explore_database_structure(engine):
    """Explore the database structure and available tables"""
    try:
        with engine.connect() as conn:
            print("\n🔍 Exploring database structure...")
            
            # Get all tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            
            if not tables:
                print("No tables found in the database")
                return
            
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f"  - {table}")
            
            return tables
            
    except Exception as e:
        print(f"❌ Error exploring database structure: {str(e)}")
        return []

def check_table_data(engine, tables=None):
    """Check what data is available in the tables"""
    try:
        # Default tables to check if none provided
        if tables is None:
            tables_to_check = [
                'schools',
                'school_sources', 
                'location_points',
                'school_locations',
                'school_directory',
                'school_names',
                'school_characteristics',
                'school_grades_offered',
                'school_enrollments',
                'school_frl'
            ]
        else:
            tables_to_check = tables
        
        print(f"\n📊 Checking data in tables...")
        
        with engine.connect() as conn:
            data_summary = {}
            
            for table in tables_to_check:
                try:
                    # Check if table exists and get row count
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    data_summary[table] = count
                    print(f"  {table}: {count:,} records")
                    
                    # If table has data, show a sample of column names
                    if count > 0:
                        columns_result = conn.execute(text(f"""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = '{table}' 
                            ORDER BY ordinal_position
                        """))
                        columns = [row[0] for row in columns_result]
                        print(f"    Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                        
                except Exception as e:
                    data_summary[table] = 0
                    print(f"  {table}: Table not found or error ({str(e)})")
            
            return data_summary
            
    except Exception as e:
        print(f"❌ Error checking table data: {str(e)}")
        return {}

def sample_data_from_tables(engine, tables_with_data, sample_size=3):
    """Get sample data from tables that have records"""
    print(f"\n🔬 Sampling data from tables with records...")
    
    try:
        with engine.connect() as conn:
            for table, count in tables_with_data.items():
                if count > 0:
                    print(f"\n--- Sample from {table} ({count:,} total records) ---")
                    try:
                        result = conn.execute(text(f"SELECT * FROM {table} LIMIT {sample_size}"))
                        rows = result.fetchall()
                        columns = result.keys()
                        
                        if rows:
                            # Print header
                            print(f"  {' | '.join(str(col)[:15].ljust(15) for col in columns)}")
                            print(f"  {'-' * (16 * len(columns))}")
                            
                            # Print sample rows
                            for row in rows:
                                row_str = ' | '.join(str(val)[:15].ljust(15) if val is not None else 'NULL'.ljust(15) for val in row)
                                print(f"  {row_str}")
                        else:
                            print("  No sample data available")
                            
                    except Exception as e:
                        print(f"  Error sampling {table}: {str(e)}")
                        
    except Exception as e:
        print(f"❌ Error sampling data: {str(e)}")

def main():
    global proxy_process
    
    try:
        print("🔍 Starting database connection test...")
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"⚠️  Service account file not found: {SERVICE_ACCOUNT_FILE}")
            print("Please ensure the service account key file is in the correct location.")
            return 1
        
        # Start Cloud SQL Proxy
        print("🚀 Starting Cloud SQL Proxy...")
        proxy_process, port = start_cloud_sql_proxy()
        
        # Test connection
        success, connection_string, engine = test_connection(port)
        if not success:
            return 1
        
        # Explore database structure
        all_tables = explore_database_structure(engine)
        
        # Check data in tables
        data_summary = check_table_data(engine, all_tables)
        
        # Sample data from tables that have records
        tables_with_data = {k: v for k, v in data_summary.items() if v > 0}
        if tables_with_data:
            sample_data_from_tables(engine, tables_with_data)
        else:
            print("\n📭 No tables contain data yet.")
        
        print(f"\n✅ Database test completed successfully!")
        print(f"📈 Summary: Found {len(all_tables)} tables, {len(tables_with_data)} contain data")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Process interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup
        if proxy_process:
            print("Stopping Cloud SQL Proxy...")
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    print("="*60)
    print("DATABASE CONNECTION TEST")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result) 