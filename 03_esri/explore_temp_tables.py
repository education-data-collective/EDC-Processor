#!/usr/bin/env python3
"""
Explore temp migration tables to understand the data structure
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

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = 'etl-service-account-key.json'
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
                '-max_connections=10',
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

def create_connection(port):
    """Create database connection"""
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        print(f"Creating connection: postgresql://{DB_USER}:***@localhost:{port}/{DB_NAME}")
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Database connection successful!")
            return engine
            
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        raise

def explore_tables(engine):
    """Explore the temp migration tables"""
    with engine.connect() as conn:
        
        # Check temp_esri_migration table
        print('\n' + '='*60)
        print('TEMP_ESRI_MIGRATION TABLE')
        print('='*60)
        
        try:
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'temp_esri_migration'
                ORDER BY ordinal_position
            """)).fetchall()
            
            if result:
                print('\nTable structure:')
                for row in result:
                    print(f'  {row[0]}: {row[1]} (nullable: {row[2]})')
                
                # Sample data
                print('\nSample data (first 5 rows):')
                sample = conn.execute(text('SELECT * FROM temp_esri_migration LIMIT 5')).fetchall()
                for i, row in enumerate(sample):
                    print(f'  Row {i+1}: {row}')
                    
                # Count records
                count = conn.execute(text('SELECT COUNT(*) FROM temp_esri_migration')).scalar()
                print(f'\nTotal records: {count:,}')
                
            else:
                print('Table not found')
                
        except Exception as e:
            print(f'Error exploring temp_esri_migration: {str(e)}')
        
        # Check temp_esri_migration_location_mapping table
        print('\n' + '='*60)
        print('TEMP_ESRI_MIGRATION_LOCATION_MAPPING TABLE')
        print('='*60)
        
        try:
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'temp_esri_migration_location_mapping'
                ORDER BY ordinal_position
            """)).fetchall()
            
            if result:
                print('\nTable structure:')
                for row in result:
                    print(f'  {row[0]}: {row[1]} (nullable: {row[2]})')
                
                # Sample data
                print('\nSample data (first 5 rows):')
                sample = conn.execute(text('SELECT * FROM temp_esri_migration_location_mapping LIMIT 5')).fetchall()
                for i, row in enumerate(sample):
                    print(f'  Row {i+1}: {row}')
                    
                # Count records
                count = conn.execute(text('SELECT COUNT(*) FROM temp_esri_migration_location_mapping')).scalar()
                print(f'\nTotal records: {count:,}')
                
            else:
                print('Table not found')
                
        except Exception as e:
            print(f'Error exploring temp_esri_migration_location_mapping: {str(e)}')
        
        # Compare record counts
        print('\n' + '='*60)
        print('RECORD COUNTS COMPARISON')
        print('='*60)
        
        tables = ['temp_esri_migration', 'temp_esri_migration_location_mapping', 'esri_demographic_data']
        for table in tables:
            try:
                count = conn.execute(text(f'SELECT COUNT(*) FROM {table}')).scalar()
                print(f'  {table}: {count:,} records')
            except Exception as e:
                print(f'  {table}: Error ({str(e)})')
        
        # Check if temp tables have data that's not in main table
        print('\n' + '='*60)
        print('DATA ALIGNMENT ANALYSIS')
        print('='*60)
        
        try:
            # Try to understand the relationship between tables
            result = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT tm.location_id) as temp_locations,
                    COUNT(DISTINCT ed.location_id) as esri_locations,
                    COUNT(DISTINCT tm.location_id) FILTER (WHERE ed.location_id IS NOT NULL) as overlap_locations
                FROM temp_esri_migration tm
                LEFT JOIN esri_demographic_data ed ON tm.location_id = ed.location_id
            """)).fetchone()
            
            if result:
                print(f'  Locations in temp_esri_migration: {result[0]:,}')
                print(f'  Locations in esri_demographic_data: {result[1]:,}')
                print(f'  Overlapping locations: {result[2]:,}')
                print(f'  Locations only in temp table: {result[0] - result[2]:,}')
                
        except Exception as e:
            print(f'  Error in alignment analysis: {str(e)}')

def main():
    """Main execution function"""
    proxy_process = None
    
    try:
        print("🚀 Starting Temp Tables Exploration")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Explore tables
        explore_tables(engine)
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during exploration: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    sys.exit(main()) 