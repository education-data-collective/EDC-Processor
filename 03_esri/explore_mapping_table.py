#!/usr/bin/env python3
"""
Explore temp_esri_migration_location_mapping table to understand the data alignment
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

def analyze_mapping_table(engine):
    """Analyze the temp_esri_migration_location_mapping table"""
    with engine.connect() as conn:
        
        print('\n' + '='*80)
        print('TEMP_ESRI_MIGRATION_LOCATION_MAPPING ANALYSIS')
        print('='*80)
        
        # Basic counts
        print('\n📊 BASIC COUNTS:')
        total_records = conn.execute(text('SELECT COUNT(*) FROM temp_esri_migration_location_mapping')).scalar()
        unique_ncessch = conn.execute(text('SELECT COUNT(DISTINCT ncessch) FROM temp_esri_migration_location_mapping')).scalar()
        unique_locations = conn.execute(text('SELECT COUNT(DISTINCT location_id) FROM temp_esri_migration_location_mapping')).scalar()
        
        print(f'  Total mapping records: {total_records:,}')
        print(f'  Unique NCES schools: {unique_ncessch:,}')
        print(f'  Unique location IDs: {unique_locations:,}')
        
        # Mapping types
        print('\n📋 MAPPING TYPES:')
        mapping_types = conn.execute(text("""
            SELECT mapping_type, COUNT(*) as count
            FROM temp_esri_migration_location_mapping
            GROUP BY mapping_type
            ORDER BY count DESC
        """)).fetchall()
        
        for row in mapping_types:
            print(f'  {row[0]}: {row[1]:,} records')
        
        # Drive time distribution
        print('\n⏱️  DRIVE TIME DISTRIBUTION:')
        drive_times = conn.execute(text("""
            SELECT drive_time, COUNT(*) as count
            FROM temp_esri_migration_location_mapping
            GROUP BY drive_time
            ORDER BY drive_time
        """)).fetchall()
        
        for row in drive_times:
            print(f'  {row[0]} minutes: {row[1]:,} records')
        
        # Check overlap with current esri_demographic_data
        print('\n🔍 OVERLAP WITH CURRENT ESRI DATA:')
        try:
            overlap_query = text("""
                SELECT 
                    COUNT(DISTINCT tm.location_id) as mapping_locations,
                    COUNT(DISTINCT ed.location_id) as esri_locations,
                    COUNT(DISTINCT tm.location_id) FILTER (WHERE ed.location_id IS NOT NULL) as overlap_locations
                FROM temp_esri_migration_location_mapping tm
                LEFT JOIN esri_demographic_data ed ON tm.location_id = ed.location_id
            """)
            
            overlap_result = conn.execute(overlap_query).fetchone()
            print(f'  Locations in mapping table: {overlap_result[0]:,}')
            print(f'  Locations in esri_demographic_data: {overlap_result[1]:,}')
            print(f'  Overlapping locations: {overlap_result[2]:,}')
            print(f'  Locations only in mapping table: {overlap_result[0] - overlap_result[2]:,}')
            
        except Exception as e:
            print(f'  Error in overlap analysis: {str(e)}')
        
        # Sample of schools that might be missing ESRI data but exist in mapping table
        print('\n🚨 POTENTIAL MISSING ESRI DATA:')
        try:
            missing_query = text("""
                SELECT DISTINCT 
                    tm.ncessch,
                    tm.location_id,
                    tm.esri_latitude,
                    tm.esri_longitude
                FROM temp_esri_migration_location_mapping tm
                LEFT JOIN esri_demographic_data ed ON tm.location_id = ed.location_id
                WHERE ed.location_id IS NULL
                LIMIT 10
            """)
            
            missing_results = conn.execute(missing_query).fetchall()
            if missing_results:
                print('  Schools with mapping but no ESRI data (sample):')
                for row in missing_results:
                    print(f'    NCES: {row[0]}, Location: {row[1]}, Coords: ({row[2]}, {row[3]})')
            else:
                print('  All mapped locations have ESRI data')
                
        except Exception as e:
            print(f'  Error finding missing data: {str(e)}')
        
        # Check if any EDC schools are in the mapping table
        print('\n🏫 EDC SCHOOLS IN MAPPING TABLE:')
        try:
            # Load EDC schools
            EDC_SCHOOLS_PATH = 'edc_schools/firebase_schools_06152025.csv'
            if os.path.exists(EDC_SCHOOLS_PATH):
                import csv
                edc_schools = set()
                with open(EDC_SCHOOLS_PATH, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        school_id = row['school_id'].strip()
                        if school_id:
                            edc_schools.add(school_id)
                
                # Check how many EDC schools are in mapping table
                edc_placeholders = ','.join([f':school_{i}' for i in range(min(len(edc_schools), 100))])  # Limit to avoid query issues
                edc_params = {f'school_{i}': school_id for i, school_id in enumerate(list(edc_schools)[:100])}
                
                edc_in_mapping = conn.execute(text(f"""
                    SELECT COUNT(DISTINCT ncessch) 
                    FROM temp_esri_migration_location_mapping 
                    WHERE ncessch IN ({edc_placeholders})
                """), edc_params).scalar()
                
                print(f'  Total EDC schools: {len(edc_schools):,}')
                print(f'  EDC schools in mapping table: {edc_in_mapping:,}')
                
                # Sample of EDC schools in mapping table
                edc_sample = conn.execute(text(f"""
                    SELECT ncessch, location_id, COUNT(*) as records
                    FROM temp_esri_migration_location_mapping 
                    WHERE ncessch IN ({edc_placeholders})
                    GROUP BY ncessch, location_id
                    LIMIT 5
                """), edc_params).fetchall()
                
                if edc_sample:
                    print('  Sample EDC schools in mapping:')
                    for row in edc_sample:
                        print(f'    NCES: {row[0]}, Location: {row[1]}, Records: {row[2]}')
                        
            else:
                print('  EDC schools CSV not found')
                
        except Exception as e:
            print(f'  Error checking EDC schools: {str(e)}')

def main():
    """Main execution function"""
    proxy_process = None
    
    try:
        print("🚀 Starting Mapping Table Analysis")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Analyze mapping table
        analyze_mapping_table(engine)
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    sys.exit(main()) 