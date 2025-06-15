#!/usr/bin/env python3
"""
ESRI Demographic Data Processing Script

This script processes ESRI demographic data for EDC schools by:
1. Finding EDC schools from CSV that need ESRI processing
2. Running ESRI data collection for their location_points
3. Validating data completeness
4. Updating the processing_status table

Features:
- Processes EDC schools using existing location_points
- Uses existing ESRI fetch functionality
- Updates processing_status.esri_processed flag
- Comprehensive validation and error handling
"""

import os
import sys
import subprocess
import time
import socket
import signal
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import csv

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy")
    sys.exit(1)

# Import the existing ESRI fetch functionality
from fetch import fetch_esri_data

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = '../etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# EDC Schools CSV path
EDC_SCHOOLS_PATH = '../edc_schools/firebase_schools_06152025.csv'

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

def load_edc_schools():
    """Load EDC schools from CSV file"""
    try:
        if not os.path.exists(EDC_SCHOOLS_PATH):
            print(f"⚠️  EDC schools file not found: {EDC_SCHOOLS_PATH}")
            return set()
        
        edc_schools = set()
        with open(EDC_SCHOOLS_PATH, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                school_id = row['school_id'].strip()
                if school_id:
                    edc_schools.add(school_id)
        
        print(f"📊 Loaded {len(edc_schools)} unique EDC schools from CSV")
        return edc_schools
        
    except Exception as e:
        print(f"❌ Error loading EDC schools: {str(e)}")
        return set()

def create_demographic_table(engine):
    """Create the esri_demographic_data table if it doesn't exist"""
    try:
        with engine.connect() as conn:
            # Check if table exists
            check_table = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'esri_demographic_data'
                );
            """)
            
            table_exists = conn.execute(check_table).scalar()
            
            if not table_exists:
                print("Creating esri_demographic_data table...")
                
                create_table_sql = text("""
                    CREATE TABLE esri_demographic_data (
                        id SERIAL PRIMARY KEY,
                        location_id INTEGER NOT NULL,
                        latitude DECIMAL(10, 8) NOT NULL,
                        longitude DECIMAL(11, 8) NOT NULL,
                        drive_time INTEGER NOT NULL,
                        
                        -- Raw demographic data
                        age4_cy DECIMAL, age5_cy DECIMAL, age6_cy DECIMAL, age7_cy DECIMAL,
                        age8_cy DECIMAL, age9_cy DECIMAL, age10_cy DECIMAL, age11_cy DECIMAL,
                        age12_cy DECIMAL, age13_cy DECIMAL, age14_cy DECIMAL, age15_cy DECIMAL,
                        age16_cy DECIMAL, age17_cy DECIMAL,
                        
                        -- Future year data
                        age4_fy DECIMAL, age5_fy DECIMAL, age6_fy DECIMAL, age7_fy DECIMAL,
                        age8_fy DECIMAL, age9_fy DECIMAL, age10_fy DECIMAL, age11_fy DECIMAL,
                        age12_fy DECIMAL, age13_fy DECIMAL, age14_fy DECIMAL, age15_fy DECIMAL,
                        age16_fy DECIMAL, age17_fy DECIMAL,
                        
                        -- 2020 Census data
                        age4_c20 DECIMAL, age5_c20 DECIMAL, age6_c20 DECIMAL, age7_c20 DECIMAL,
                        age8_c20 DECIMAL, age9_c20 DECIMAL, age10_c20 DECIMAL, age11_c20 DECIMAL,
                        age12_c20 DECIMAL, age13_c20 DECIMAL, age14_c20 DECIMAL, age15_c20 DECIMAL,
                        age16_c20 DECIMAL, age17_c20 DECIMAL,
                        
                        -- Adult demographic percentages (2020)
                        per_hisp_adult_20 DECIMAL, per_wht_adult_20 DECIMAL, per_blk_adult_20 DECIMAL,
                        per_asn_adult_20 DECIMAL, per_pi_adult_20 DECIMAL, per_ai_adult_20 DECIMAL,
                        per_other_adult_20 DECIMAL, per_two_or_more_adult_20 DECIMAL,
                        
                        -- Child demographic percentages (2020)
                        per_hisp_child_20 DECIMAL, per_wht_child_20 DECIMAL, per_blk_child_20 DECIMAL,
                        per_asn_child_20 DECIMAL, per_pi_child_20 DECIMAL, per_ai_child_20 DECIMAL,
                        per_other_child_20 DECIMAL, per_two_or_more_child_20 DECIMAL,
                        
                        -- Economic data
                        medhinc_cy DECIMAL, per_50k_cy DECIMAL, per_renter_cy DECIMAL, per_vacant_cy DECIMAL,
                        
                        -- Spatial data
                        drive_time_polygon JSONB,
                        
                        -- ESRI Metadata
                        source_country VARCHAR(50),
                        area_type VARCHAR(50),
                        buffer_units VARCHAR(20),
                        buffer_units_alias VARCHAR(50),
                        buffer_radii FLOAT,
                        aggregation_method VARCHAR(50),
                        population_to_polygon_size_rating FLOAT,
                        apportionment_confidence FLOAT,
                        has_data INTEGER DEFAULT 1,
                        
                        -- Spatial data
                        drive_time_polygon TEXT,
                        
                        -- Timestamp
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        -- Constraints
                        UNIQUE(location_id, drive_time)
                    );
                """)
                
                conn.execute(create_table_sql)
                conn.commit()
                
                # Create indexes
                conn.execute(text("CREATE INDEX idx_esri_location_id ON esri_demographic_data(location_id);"))
                conn.execute(text("CREATE INDEX idx_esri_drive_time ON esri_demographic_data(drive_time);"))
                conn.execute(text("CREATE INDEX idx_esri_timestamp ON esri_demographic_data(timestamp);"))
                conn.commit()
                
                print("✅ Created esri_demographic_data table with indexes")
            
    except Exception as e:
        print(f"❌ Error creating demographic table: {str(e)}")
        raise

def get_edc_schools_needing_esri_processing(engine, edc_schools, force_refresh=False):
    """Get EDC schools that need ESRI processing"""
    try:
        if not edc_schools:
            return []
        
        with engine.connect() as conn:
            placeholders = ','.join([f':school_{i}' for i in range(len(edc_schools))])
            params = {f'school_{i}': school_id for i, school_id in enumerate(edc_schools)}
            
            # Find EDC schools with locations but missing/incomplete ESRI data
            if force_refresh:
                # Force refresh - process all EDC schools with locations
                condition = "TRUE"
            else:
                # Only process schools without recent ESRI data (within 30 days) or incomplete data
                condition = """
                    (ed.location_id IS NULL OR 
                     ed.timestamp < CURRENT_DATE - INTERVAL '30 days' OR
                     COUNT(ed.id) < 3)  -- Missing some drive times
                """
            
            result = conn.execute(text(f"""
                SELECT 
                    sl.school_id,
                    sl.location_id,
                    lp.latitude,
                    lp.longitude,
                    COUNT(ed.id) as existing_esri_records,
                    MAX(ed.timestamp) as latest_processing
                FROM school_locations sl
                JOIN location_points lp ON sl.location_id = lp.id
                LEFT JOIN esri_demographic_data ed ON ed.location_id = lp.id
                WHERE sl.school_id IN ({placeholders})
                  AND lp.latitude IS NOT NULL 
                  AND lp.longitude IS NOT NULL
                GROUP BY sl.school_id, sl.location_id, lp.latitude, lp.longitude
                HAVING {condition}
                ORDER BY sl.school_id
            """), params).fetchall()
            
            return result
            
    except Exception as e:
        print(f"❌ Error getting schools needing ESRI processing: {str(e)}")
        return []

def store_esri_data(engine, location_id, latitude, longitude, esri_data):
    """Store ESRI data in the database"""
    try:
        with engine.begin() as conn:
            # Clear existing data for this location
            conn.execute(text("""
                DELETE FROM esri_demographic_data 
                WHERE location_id = :location_id
            """), {'location_id': location_id})
            
            # Insert new data for each drive time
            for drive_time in [5, 10, 15]:
                data = esri_data.get(drive_time, {})
                
                insert_sql = text("""
                    INSERT INTO esri_demographic_data (
                        location_id, latitude, longitude, drive_time,
                        age4_cy, age5_cy, age6_cy, age7_cy, age8_cy, age9_cy,
                        age10_cy, age11_cy, age12_cy, age13_cy, age14_cy, age15_cy, age16_cy, age17_cy,
                        age4_fy, age5_fy, age6_fy, age7_fy, age8_fy, age9_fy,
                        age10_fy, age11_fy, age12_fy, age13_fy, age14_fy, age15_fy, age16_fy, age17_fy,
                        age4_c20, age5_c20, age6_c20, age7_c20, age8_c20, age9_c20,
                        age10_c20, age11_c20, age12_c20, age13_c20, age14_c20, age15_c20, age16_c20, age17_c20,
                        per_hisp_adult_20, per_wht_adult_20, per_blk_adult_20, per_asn_adult_20,
                        per_pi_adult_20, per_ai_adult_20, per_other_adult_20, per_two_or_more_adult_20,
                        per_hisp_child_20, per_wht_child_20, per_blk_child_20, per_asn_child_20,
                        per_pi_child_20, per_ai_child_20, per_other_child_20, per_two_or_more_child_20,
                        medhinc_cy, per_50k_cy, per_renter_cy, per_vacant_cy,
                        drive_time_polygon, has_data
                    ) VALUES (
                        :location_id, :latitude, :longitude, :drive_time,
                        :age4_cy, :age5_cy, :age6_cy, :age7_cy, :age8_cy, :age9_cy,
                        :age10_cy, :age11_cy, :age12_cy, :age13_cy, :age14_cy, :age15_cy, :age16_cy, :age17_cy,
                        :age4_fy, :age5_fy, :age6_fy, :age7_fy, :age8_fy, :age9_fy,
                        :age10_fy, :age11_fy, :age12_fy, :age13_fy, :age14_fy, :age15_fy, :age16_fy, :age17_fy,
                        :age4_c20, :age5_c20, :age6_c20, :age7_c20, :age8_c20, :age9_c20,
                        :age10_c20, :age11_c20, :age12_c20, :age13_c20, :age14_c20, :age15_c20, :age16_c20, :age17_c20,
                        :per_hisp_adult_20, :per_wht_adult_20, :per_blk_adult_20, :per_asn_adult_20,
                        :per_pi_adult_20, :per_ai_adult_20, :per_other_adult_20, :per_two_or_more_adult_20,
                        :per_hisp_child_20, :per_wht_child_20, :per_blk_child_20, :per_asn_child_20,
                        :per_pi_child_20, :per_ai_child_20, :per_other_child_20, :per_two_or_more_child_20,
                        :medhinc_cy, :per_50k_cy, :per_renter_cy, :per_vacant_cy,
                        :drive_time_polygon, :has_data
                    )
                """)
                
                params = {
                    'location_id': location_id,
                    'latitude': float(latitude),
                    'longitude': float(longitude),
                    'drive_time': drive_time,
                    'has_data': bool(data)
                }
                
                # Add all the demographic data fields
                for field in [
                    'age4_cy', 'age5_cy', 'age6_cy', 'age7_cy', 'age8_cy', 'age9_cy',
                    'age10_cy', 'age11_cy', 'age12_cy', 'age13_cy', 'age14_cy', 'age15_cy', 'age16_cy', 'age17_cy',
                    'age4_fy', 'age5_fy', 'age6_fy', 'age7_fy', 'age8_fy', 'age9_fy',
                    'age10_fy', 'age11_fy', 'age12_fy', 'age13_fy', 'age14_fy', 'age15_fy', 'age16_fy', 'age17_fy',
                    'age4_c20', 'age5_c20', 'age6_c20', 'age7_c20', 'age8_c20', 'age9_c20',
                    'age10_c20', 'age11_c20', 'age12_c20', 'age13_c20', 'age14_c20', 'age15_c20', 'age16_c20', 'age17_c20',
                    'per_hisp_adult_20', 'per_wht_adult_20', 'per_blk_adult_20', 'per_asn_adult_20',
                    'per_pi_adult_20', 'per_ai_adult_20', 'per_other_adult_20', 'per_two_or_more_adult_20',
                    'per_hisp_child_20', 'per_wht_child_20', 'per_blk_child_20', 'per_asn_child_20',
                    'per_pi_child_20', 'per_ai_child_20', 'per_other_child_20', 'per_two_or_more_child_20',
                    'medhinc_cy', 'per_50k_cy', 'per_renter_cy', 'per_vacant_cy'
                ]:
                    params[field] = data.get(field)
                
                # Handle polygon data
                polygon_data = data.get('drive_time_polygon')
                if polygon_data:
                    import json
                    params['drive_time_polygon'] = json.dumps(polygon_data)
                else:
                    params['drive_time_polygon'] = None
                
                conn.execute(insert_sql, params)
            
            print(f"✅ Stored ESRI data for location_id {location_id}")
            return True
            
    except Exception as e:
        print(f"❌ Error storing ESRI data for location_id {location_id}: {str(e)}")
        return False

def validate_esri_data_completeness(engine, location_id):
    """Validate that ESRI data is complete for a location"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN has_data THEN 1 END) as records_with_data,
                    COUNT(DISTINCT drive_time) as unique_drive_times,
                    COUNT(CASE WHEN medhinc_cy IS NOT NULL THEN 1 END) as income_records,
                    COUNT(CASE WHEN per_hisp_child_20 IS NOT NULL THEN 1 END) as demographic_records
                FROM esri_demographic_data
                WHERE location_id = :location_id
            """), {'location_id': location_id}).fetchone()
            
            if not result:
                return False
            
            # Consider complete if we have all 3 drive times with data
            is_complete = (result[0] == 3 and  # 3 records (5, 10, 15 min)
                          result[1] >= 1 and   # At least some records have data  
                          result[2] == 3)      # All 3 drive times present
            
            return is_complete
            
    except Exception as e:
        print(f"❌ Error validating ESRI data for location_id {location_id}: {str(e)}")
        return False

def update_processing_status(engine, school_id, data_year, esri_processed):
    """Update the processing_status table for a school"""
    try:
        with engine.begin() as conn:
            # Check if record exists
            existing = conn.execute(text("""
                SELECT id FROM processing_status 
                WHERE school_id = :school_id AND data_year = :data_year
            """), {'school_id': school_id, 'data_year': data_year}).fetchone()
            
            if existing:
                # Update existing record
                conn.execute(text("""
                    UPDATE processing_status 
                    SET esri_processed = :esri_processed,
                        last_processed_at = CURRENT_TIMESTAMP
                    WHERE school_id = :school_id AND data_year = :data_year
                """), {
                    'school_id': school_id,
                    'data_year': data_year,
                    'esri_processed': esri_processed
                })
            else:
                # Create new record
                conn.execute(text("""
                    INSERT INTO processing_status (
                        school_id, data_year, esri_processed, created_at, last_processed_at
                    ) VALUES (
                        :school_id, :data_year, :esri_processed, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """), {
                    'school_id': school_id,
                    'data_year': data_year,
                    'esri_processed': esri_processed
                })
            
            return True
            
    except Exception as e:
        print(f"❌ Error updating processing status for school {school_id}: {str(e)}")
        return False

def process_esri_for_school(engine, school_id, location_id, latitude, longitude, data_year):
    """Process ESRI data for a single school"""
    try:
        print(f"📍 Processing ESRI data for school {school_id} (location_id: {location_id})")
        
        # Fetch ESRI data
        esri_data = fetch_esri_data(latitude, longitude)
        
        if not esri_data:
            print(f"⚠️  No ESRI data returned for school {school_id}")
            # Still update status to mark as attempted
            update_processing_status(engine, school_id, data_year, False)
            return False
        
        # Store ESRI data
        success = store_esri_data(engine, location_id, latitude, longitude, esri_data)
        
        if not success:
            print(f"❌ Failed to store ESRI data for school {school_id}")
            update_processing_status(engine, school_id, data_year, False)
            return False
        
        # Validate completeness
        is_complete = validate_esri_data_completeness(engine, location_id)
        
        # Update processing status
        update_success = update_processing_status(engine, school_id, data_year, is_complete)
        
        if update_success:
            print(f"✅ Successfully processed ESRI data for school {school_id} (complete: {is_complete})")
        else:
            print(f"⚠️  Processed ESRI data but failed to update status for school {school_id}")
        
        return is_complete
        
    except Exception as e:
        print(f"❌ Error processing ESRI data for school {school_id}: {str(e)}")
        update_processing_status(engine, school_id, data_year, False)
        return False

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Process ESRI demographic data for EDC schools')
    parser.add_argument('--force-refresh', action='store_true', 
                       help='Force refresh of all EDC schools (ignore 30-day cache)')
    parser.add_argument('--limit', type=int, help='Limit number of schools to process (for testing)')
    parser.add_argument('--data-year', type=int, default=2024, 
                       help='Data year to use for processing_status updates (default: 2024)')
    
    args = parser.parse_args()
    
    proxy_process = None
    
    try:
        print("🚀 Starting ESRI Demographic Data Processing for EDC Schools")
        print(f"📅 Processing time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Data year: {args.data_year}")
        if args.force_refresh:
            print("⚡ Force refresh mode enabled")
        if args.limit:
            print(f"🔢 Processing limited to {args.limit} schools")
        
        # Load EDC schools
        edc_schools = load_edc_schools()
        
        if not edc_schools:
            print("❌ No EDC schools loaded. Exiting.")
            return 1
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Ensure demographic table exists
        create_demographic_table(engine)
        
        # Get schools needing processing
        schools_to_process = get_edc_schools_needing_esri_processing(
            engine, edc_schools, args.force_refresh
        )
        
        if not schools_to_process:
            print("✅ No EDC schools need ESRI processing at this time")
            return 0
        
        if args.limit:
            schools_to_process = schools_to_process[:args.limit]
        
        print(f"📋 Found {len(schools_to_process)} EDC schools needing ESRI processing")
        
        # Process each school
        successful_count = 0
        failed_count = 0
        
        for i, (school_id, location_id, latitude, longitude, existing_records, latest_processing) in enumerate(schools_to_process, 1):
            print(f"\n[{i}/{len(schools_to_process)}] Processing {school_id}...")
            
            success = process_esri_for_school(
                engine, school_id, location_id, latitude, longitude, args.data_year
            )
            
            if success:
                successful_count += 1
            else:
                failed_count += 1
            
            # Rate limiting - wait between API calls
            if i < len(schools_to_process):
                print("⏳ Waiting 2 seconds between requests...")
                time.sleep(2)
        
        # Summary
        print(f"\n" + "="*60)
        print(f"📊 PROCESSING SUMMARY")
        print(f"="*60)
        print(f"✅ Successfully processed: {successful_count}")
        print(f"❌ Failed to process: {failed_count}")
        print(f"📋 Total attempted: {len(schools_to_process)}")
        
        if successful_count > 0:
            success_rate = (successful_count / len(schools_to_process)) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        return 0 if failed_count == 0 else 1
        
    except Exception as e:
        print(f"❌ Error during processing: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    sys.exit(main()) 