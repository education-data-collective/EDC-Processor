#!/usr/bin/env python3
"""
Nearby Schools Processing Script

This script processes nearby schools data for locations by:
1. Finding locations that need nearby schools processing
2. Using existing ESRI polygon data to identify nearby schools
3. Populating school_polygon_relationships and nearby_school_polygons tables
4. Validating data completeness and updating processing status

Features:
- Processes locations using existing ESRI polygon data
- Uses existing school detection functionality
- Updates school_polygon_relationships and nearby_school_polygons tables
- Comprehensive validation and error handling
- Batch processing capabilities
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

# Import the nearby schools processing functionality
try:
    from fetch import process_nearby_schools_for_location, validate_nearby_school_results
except ImportError:
    print("fetch module not found. Processing functionality will be limited.")
    process_nearby_schools_for_location = None
    validate_nearby_school_results = None

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = '../etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# EDC Schools CSV path
EDC_SCHOOLS_PATH = '../edc_schools/firebase_data/edc_schools.csv'

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

def find_edc_locations_needing_processing(engine, data_year, edc_schools, force_refresh=False):
    """Find EDC school locations that need nearby schools processing using sophisticated matching"""
    try:
        # Separate hyphenated and non-hyphenated school IDs (from 03_esri approach)
        hyphenated_schools = []
        non_hyphenated_schools = []
        
        for school_id in edc_schools:
            if '-' in school_id:
                parts = school_id.split('-')
                if len(parts) == 2:
                    nces_id, suffix = parts
                    hyphenated_schools.append((nces_id, suffix, school_id))
            else:
                non_hyphenated_schools.append(school_id)
        
        print(f"🔍 EDC Schools: {len(hyphenated_schools)} hyphenated, {len(non_hyphenated_schools)} non-hyphenated")
        
        with engine.connect() as conn:
            all_locations = []
            
            # Handle hyphenated schools (match ncessch + split_suffix)
            if hyphenated_schools:
                hyphenated_conditions = []
                params = {'data_year': data_year}
                
                for i, (nces_id, suffix, original_id) in enumerate(hyphenated_schools):
                    hyphenated_conditions.append(f"(sd.ncessch = :nces_{i} AND sd.split_suffix = :suffix_{i})")
                    params[f'nces_{i}'] = nces_id
                    params[f'suffix_{i}'] = suffix
                
                hyphenated_query = f"""
                    SELECT DISTINCT
                        lp.id as location_id,
                        lp.latitude,
                        lp.longitude,
                        CONCAT(sd.ncessch, '-', sd.split_suffix) as school_id,
                        s.uuid as school_uuid,
                        sl.data_year,
                        CASE WHEN spr.id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_nearby_data,
                        CASE WHEN esri.location_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_esri_data
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                        AND spr.data_year = :data_year
                    LEFT JOIN (
                        SELECT DISTINCT location_id 
                        FROM esri_demographic_data
                        WHERE drive_time_polygon IS NOT NULL
                    ) esri ON lp.id = esri.location_id
                    WHERE ({' OR '.join(hyphenated_conditions)})
                        AND sd.is_current = true
                        AND sl.data_year = :data_year
                        AND sl.is_current = true
                        AND lp.latitude IS NOT NULL 
                        AND lp.longitude IS NOT NULL
                """
                
                # Add filtering conditions
                if not force_refresh:
                    hyphenated_query += " AND spr.id IS NULL"
                hyphenated_query += " AND esri.location_id IS NOT NULL"
                
                hyphenated_results = conn.execute(text(hyphenated_query), params).fetchall()
                
                for row in hyphenated_results:
                    all_locations.append({
                        'location_id': row[0],
                        'latitude': float(row[1]),
                        'longitude': float(row[2]),
                        'school_id': row[3],
                        'school_uuid': row[4],
                        'data_year': row[5],
                        'has_nearby_data': row[6],
                        'has_esri_data': row[7]
                    })
                
                print(f"✅ Found {len(hyphenated_results)} hyphenated EDC school locations")
            
            # Handle non-hyphenated schools (direct ncessch match)
            if non_hyphenated_schools:
                non_hyphenated_query = """
                    SELECT DISTINCT
                        lp.id as location_id,
                        lp.latitude,
                        lp.longitude,
                        sd.ncessch as school_id,
                        s.uuid as school_uuid,
                        sl.data_year,
                        CASE WHEN spr.id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_nearby_data,
                        CASE WHEN esri.location_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_esri_data
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                        AND spr.data_year = :data_year
                    LEFT JOIN (
                        SELECT DISTINCT location_id 
                        FROM esri_demographic_data
                        WHERE drive_time_polygon IS NOT NULL
                    ) esri ON lp.id = esri.location_id
                    WHERE sd.ncessch = ANY(:school_ids)
                        AND sd.is_current = true
                        AND sl.data_year = :data_year
                        AND sl.is_current = true
                        AND lp.latitude IS NOT NULL 
                        AND lp.longitude IS NOT NULL
                """
                
                # Add filtering conditions
                if not force_refresh:
                    non_hyphenated_query += " AND spr.id IS NULL"
                non_hyphenated_query += " AND esri.location_id IS NOT NULL"
                
                non_hyphenated_results = conn.execute(text(non_hyphenated_query), {
                    'data_year': data_year,
                    'school_ids': list(non_hyphenated_schools)
                }).fetchall()
                
                for row in non_hyphenated_results:
                    all_locations.append({
                        'location_id': row[0],
                        'latitude': float(row[1]),
                        'longitude': float(row[2]),
                        'school_id': row[3],
                        'school_uuid': row[4],
                        'data_year': row[5],
                        'has_nearby_data': row[6],
                        'has_esri_data': row[7]
                    })
                
                print(f"✅ Found {len(non_hyphenated_results)} non-hyphenated EDC school locations")
            
            # Check for mapping table fallback (if available)
            try:
                mapping_check = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'temp_esri_migration_location_mapping'
                    );
                """)).scalar()
                
                if mapping_check:
                    print("🔍 Checking mapping table for additional EDC schools...")
                    # Add mapping table logic here if needed
                    # (Similar to 03_esri approach but adapted for nearby schools processing)
                
            except Exception:
                # Mapping table doesn't exist, continue without it
                pass
            
            print(f"📍 Total EDC locations found: {len(all_locations)}")
            
            if all_locations:
                edc_count = len([loc for loc in all_locations if loc['school_id'] in edc_schools])
                print(f"  🎯 {edc_count} are confirmed EDC schools")
                
                esri_count = len([loc for loc in all_locations if loc['has_esri_data'] == 'Yes'])
                print(f"  🗺️  {esri_count} have ESRI polygon data")
            
            return all_locations
            
    except Exception as e:
        print(f"❌ Error finding EDC locations needing processing: {str(e)}")
        return []

def create_nearby_schools_tables(engine):
    """Create the nearby schools tables if they don't exist"""
    try:
        with engine.connect() as conn:
            # Check if tables exist
            tables_check = conn.execute(text("""
                SELECT 
                    table_name,
                    EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = t.table_name
                    ) as exists
                FROM (VALUES 
                    ('school_polygon_relationships'),
                    ('nearby_school_polygons')
                ) AS t(table_name)
            """)).fetchall()
            
            tables_to_create = [row[0] for row in tables_check if not row[1]]
            
            if 'school_polygon_relationships' in tables_to_create:
                print("Creating school_polygon_relationships table...")
                
                conn.execute(text("""
                    CREATE TABLE school_polygon_relationships (
                        id SERIAL PRIMARY KEY,
                        location_id INTEGER NOT NULL REFERENCES location_points(id),
                        drive_time INTEGER NOT NULL,
                        data_year INTEGER NOT NULL,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(location_id, drive_time, data_year)
                    )
                """))
                
                conn.execute(text("""
                    CREATE INDEX idx_polygon_location_year 
                    ON school_polygon_relationships(location_id, data_year)
                """))
                
                conn.commit()
                print("✅ Created school_polygon_relationships table")
            
            if 'nearby_school_polygons' in tables_to_create:
                print("Creating nearby_school_polygons table...")
                
                conn.execute(text("""
                    CREATE TABLE nearby_school_polygons (
                        id SERIAL PRIMARY KEY,
                        polygon_relationship_id INTEGER NOT NULL REFERENCES school_polygon_relationships(id),
                        school_uuid VARCHAR(36) NOT NULL,
                        relationship_type VARCHAR(20) NOT NULL DEFAULT 'nearby',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(polygon_relationship_id, school_uuid, relationship_type)
                    )
                """))
                
                conn.execute(text("""
                    CREATE INDEX idx_nearby_school_polygon_relationship 
                    ON nearby_school_polygons(polygon_relationship_id)
                """))
                
                conn.execute(text("""
                    CREATE INDEX idx_nearby_school_polygon_uuid 
                    ON nearby_school_polygons(school_uuid)
                """))
                
                conn.commit()
                print("✅ Created nearby_school_polygons table")
                
            if not tables_to_create:
                print("✅ All required tables already exist")
                
    except Exception as e:
        print(f"❌ Error creating tables: {str(e)}")
        raise

def find_locations_needing_processing(engine, data_year, edc_schools=None, force_refresh=False):
    """Find locations that need nearby schools processing"""
    try:
        with engine.connect() as conn:
            # If EDC schools specified, use the sophisticated matching approach from 03_esri
            if edc_schools:
                return find_edc_locations_needing_processing(engine, data_year, edc_schools, force_refresh)
            
            # Build query to find locations that need processing (all schools)
            base_query = """
                SELECT DISTINCT
                    lp.id as location_id,
                    lp.latitude,
                    lp.longitude,
                    COALESCE(CONCAT(sd.ncessch, '-', sd.split_suffix), sd.ncessch, sd.state_school_id) as school_id,
                    s.uuid as school_uuid,
                    sl.data_year,
                    CASE WHEN spr.id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_nearby_data,
                    CASE WHEN esri.location_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_esri_data
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                JOIN schools s ON sl.school_id = s.id
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                    AND spr.data_year = :data_year
                LEFT JOIN (
                    SELECT DISTINCT location_id 
                    FROM esri_demographic_data
                    WHERE drive_time_polygon IS NOT NULL
                ) esri ON lp.id = esri.location_id
                WHERE sl.data_year = :data_year
                    AND sl.is_current = true
                    AND lp.latitude IS NOT NULL 
                    AND lp.longitude IS NOT NULL
            """
            
            params = {'data_year': data_year}
            conditions = []
            
            # Filter based on refresh mode
            if not force_refresh:
                conditions.append("spr.id IS NULL")  # Only locations without existing data
            
            # Only include locations with ESRI data
            conditions.append("esri.location_id IS NOT NULL")
            
            if conditions:
                base_query += " AND " + " AND ".join(conditions)
            
            base_query += " ORDER BY lp.id"
            
            results = conn.execute(text(base_query), params).fetchall()
            
            locations = []
            for row in results:
                locations.append({
                    'location_id': row[0],
                    'latitude': float(row[1]),
                    'longitude': float(row[2]),
                    'school_id': row[3],
                    'school_uuid': row[4],
                    'data_year': row[5],
                    'has_nearby_data': row[6],
                    'has_esri_data': row[7]
                })
            
            print(f"📍 Found {len(locations)} locations needing nearby schools processing")
            
            if edc_schools:
                edc_count = len([loc for loc in locations if loc['school_id'] in edc_schools])
                print(f"  🎯 {edc_count} are EDC schools")
                
            esri_count = len([loc for loc in locations if loc['has_esri_data'] == 'Yes'])
            print(f"  🗺️  {esri_count} have ESRI polygon data")
            
            return locations
            
    except Exception as e:
        print(f"❌ Error finding locations needing processing: {str(e)}")
        return []

def process_location_nearby_schools(engine, location_id, data_year):
    """Process nearby schools for a single location"""
    try:
        print(f"🔍 Processing location {location_id} for year {data_year}...")
        
        # Use the imported function from fetch.py
        if process_nearby_schools_for_location:
            success = process_nearby_schools_for_location(engine, location_id, data_year)
            
            if success:
                # Validate the results
                if validate_nearby_school_results:
                    validation_result = validate_nearby_school_results(engine, location_id, data_year)
                    if validation_result['is_valid']:
                        print(f"✅ Successfully processed location {location_id}")
                        print(f"  📊 {validation_result['polygon_count']} polygons created")
                        print(f"  🏫 {validation_result['school_count']} total nearby school records")
                        print(f"  🎯 {validation_result['unique_schools']} unique nearby schools")
                        print(f"  ⏱️  Drive times: {validation_result['drive_times']}")
                        return True
                    else:
                        print(f"⚠️  Validation failed for location {location_id}: {validation_result['error']}")
                        return False
                else:
                    print(f"✅ Successfully processed location {location_id} (validation skipped)")
                    return True
            else:
                print(f"❌ Failed to process location {location_id}")
                return False
        else:
            print(f"❌ Processing function not available from fetch module")
            return False
            
    except Exception as e:
        print(f"❌ Error processing location {location_id}: {str(e)}")
        return False

def update_processing_status(engine, location_id, data_year, nearby_processed):
    """Update processing status for a location (placeholder function)"""
    # Processing status tracking removed - not needed for core functionality
    return True

def process_batch_locations(engine, locations, data_year, batch_size=10):
    """Process locations in batches"""
    try:
        total_locations = len(locations)
        processed_count = 0
        failed_count = 0
        
        print(f"🚀 Processing {total_locations} locations in batches of {batch_size}")
        
        for i in range(0, total_locations, batch_size):
            batch = locations[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_locations + batch_size - 1) // batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} locations)")
            
            batch_processed = 0
            batch_failed = 0
            
            for location in batch:
                location_id = location['location_id']
                
                try:
                    success = process_location_nearby_schools(engine, location_id, data_year)
                    
                    if success:
                        batch_processed += 1
                        # Update processing status
                        update_processing_status(engine, location_id, data_year, True)
                    else:
                        batch_failed += 1
                        update_processing_status(engine, location_id, data_year, False)
                        
                except Exception as e:
                    print(f"❌ Batch processing error for location {location_id}: {str(e)}")
                    batch_failed += 1
                    update_processing_status(engine, location_id, data_year, False)
            
            processed_count += batch_processed
            failed_count += batch_failed
            
            print(f"📊 Batch {batch_num} results: {batch_processed} success, {batch_failed} failed")
            
            # Small delay between batches to avoid overwhelming the database
            if i + batch_size < total_locations:
                time.sleep(1)
        
        print(f"\n✅ Batch processing complete:")
        print(f"  📈 Total processed: {processed_count}/{total_locations}")
        print(f"  ❌ Total failed: {failed_count}/{total_locations}")
        print(f"  📊 Success rate: {(processed_count/total_locations)*100:.1f}%")
        
        return {
            'total': total_locations,
            'processed': processed_count,
            'failed': failed_count,
            'success_rate': (processed_count/total_locations)*100 if total_locations > 0 else 0
        }
        
    except Exception as e:
        print(f"❌ Error in batch processing: {str(e)}")
        return {
            'total': len(locations),
            'processed': processed_count,
            'failed': failed_count + (len(locations) - processed_count),
            'success_rate': 0
        }

def validate_processing_completeness(engine, data_year, edc_schools=None):
    """Validate processing completeness"""
    try:
        with engine.connect() as conn:
            if edc_schools:
                # Use the sophisticated EDC matching approach
                return validate_edc_processing_completeness(engine, data_year, edc_schools)
            
            # Standard validation for all schools
            params = {'data_year': data_year}
            
            # Check processing completeness
            result = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT lp.id) as total_locations,
                    COUNT(DISTINCT CASE WHEN spr.id IS NOT NULL THEN lp.id END) as processed_locations,
                    COUNT(DISTINCT CASE WHEN esri.location_id IS NOT NULL THEN lp.id END) as locations_with_esri,
                    COUNT(DISTINCT nsp.school_uuid) as unique_nearby_schools,
                    COUNT(nsp.id) as total_nearby_records
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                JOIN schools s ON sl.school_id = s.id
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                    AND spr.data_year = :data_year
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                LEFT JOIN (
                    SELECT DISTINCT location_id 
                    FROM esri_demographic_data
                    WHERE drive_time_polygon IS NOT NULL
                ) esri ON lp.id = esri.location_id
                WHERE sl.data_year = :data_year
                    AND sl.is_current = true
                    AND lp.latitude IS NOT NULL 
                    AND lp.longitude IS NOT NULL
            """), params).fetchone()
            
            if result:
                validation = {
                    'total_locations': result[0],
                    'processed_locations': result[1],
                    'locations_with_esri': result[2],
                    'unique_nearby_schools': result[3],
                    'total_nearby_records': result[4],
                    'processing_rate': (result[1] / result[0] * 100) if result[0] > 0 else 0,
                    'esri_availability_rate': (result[2] / result[0] * 100) if result[0] > 0 else 0
                }
                
                return validation
            
    except Exception as e:
        print(f"❌ Error validating processing completeness: {str(e)}")
        return None

def validate_edc_processing_completeness(engine, data_year, edc_schools):
    """Validate processing completeness for EDC schools using sophisticated matching"""
    try:
        # Separate hyphenated and non-hyphenated school IDs
        hyphenated_schools = []
        non_hyphenated_schools = []
        
        for school_id in edc_schools:
            if '-' in school_id:
                parts = school_id.split('-')
                if len(parts) == 2:
                    nces_id, suffix = parts
                    hyphenated_schools.append((nces_id, suffix, school_id))
            else:
                non_hyphenated_schools.append(school_id)
        
        with engine.connect() as conn:
            total_locations = 0
            processed_locations = 0
            locations_with_esri = 0
            unique_nearby_schools = set()
            total_nearby_records = 0
            
            # Handle hyphenated schools
            if hyphenated_schools:
                hyphenated_conditions = []
                params = {'data_year': data_year}
                
                for i, (nces_id, suffix, original_id) in enumerate(hyphenated_schools):
                    hyphenated_conditions.append(f"(sd.ncessch = :nces_{i} AND sd.split_suffix = :suffix_{i})")
                    params[f'nces_{i}'] = nces_id
                    params[f'suffix_{i}'] = suffix
                
                hyphenated_query = f"""
                    SELECT 
                        COUNT(DISTINCT lp.id) as total_locations,
                        COUNT(DISTINCT CASE WHEN spr.id IS NOT NULL THEN lp.id END) as processed_locations,
                        COUNT(DISTINCT CASE WHEN esri.location_id IS NOT NULL THEN lp.id END) as locations_with_esri,
                        COUNT(DISTINCT nsp.school_uuid) as unique_nearby_schools,
                        COUNT(nsp.id) as total_nearby_records
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                        AND spr.data_year = :data_year
                    LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                    LEFT JOIN (
                        SELECT DISTINCT location_id 
                        FROM esri_demographic_data
                        WHERE drive_time_polygon IS NOT NULL
                    ) esri ON lp.id = esri.location_id
                    WHERE ({' OR '.join(hyphenated_conditions)})
                        AND sd.is_current = true
                        AND sl.data_year = :data_year
                        AND sl.is_current = true
                        AND lp.latitude IS NOT NULL 
                        AND lp.longitude IS NOT NULL
                """
                
                hyphenated_result = conn.execute(text(hyphenated_query), params).fetchone()
                if hyphenated_result:
                    total_locations += hyphenated_result[0] or 0
                    processed_locations += hyphenated_result[1] or 0
                    locations_with_esri += hyphenated_result[2] or 0
                    total_nearby_records += hyphenated_result[4] or 0
            
            # Handle non-hyphenated schools
            if non_hyphenated_schools:
                non_hyphenated_query = """
                    SELECT 
                        COUNT(DISTINCT lp.id) as total_locations,
                        COUNT(DISTINCT CASE WHEN spr.id IS NOT NULL THEN lp.id END) as processed_locations,
                        COUNT(DISTINCT CASE WHEN esri.location_id IS NOT NULL THEN lp.id END) as locations_with_esri,
                        COUNT(DISTINCT nsp.school_uuid) as unique_nearby_schools,
                        COUNT(nsp.id) as total_nearby_records
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                        AND spr.data_year = :data_year
                    LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                    LEFT JOIN (
                        SELECT DISTINCT location_id 
                        FROM esri_demographic_data
                        WHERE drive_time_polygon IS NOT NULL
                    ) esri ON lp.id = esri.location_id
                    WHERE sd.ncessch = ANY(:school_ids)
                        AND sd.is_current = true
                        AND sl.data_year = :data_year
                        AND sl.is_current = true
                        AND lp.latitude IS NOT NULL 
                        AND lp.longitude IS NOT NULL
                """
                
                non_hyphenated_result = conn.execute(text(non_hyphenated_query), {
                    'data_year': data_year,
                    'school_ids': list(non_hyphenated_schools)
                }).fetchone()
                
                if non_hyphenated_result:
                    total_locations += non_hyphenated_result[0] or 0
                    processed_locations += non_hyphenated_result[1] or 0
                    locations_with_esri += non_hyphenated_result[2] or 0
                    total_nearby_records += non_hyphenated_result[4] or 0
            
            # Get unique nearby schools across both queries
            unique_schools_query = """
                SELECT COUNT(DISTINCT nsp.school_uuid) as unique_nearby_schools
                FROM school_polygon_relationships spr
                JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                WHERE spr.data_year = :data_year
            """
            
            unique_result = conn.execute(text(unique_schools_query), {'data_year': data_year}).fetchone()
            unique_nearby_schools_count = unique_result[0] if unique_result else 0
            
            validation = {
                'total_locations': total_locations,
                'processed_locations': processed_locations,
                'locations_with_esri': locations_with_esri,
                'unique_nearby_schools': unique_nearby_schools_count,
                'total_nearby_records': total_nearby_records,
                'processing_rate': (processed_locations / total_locations * 100) if total_locations > 0 else 0,
                'esri_availability_rate': (locations_with_esri / total_locations * 100) if total_locations > 0 else 0
            }
            
            return validation
            
    except Exception as e:
        print(f"❌ Error validating EDC processing completeness: {str(e)}")
        return None

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Process nearby schools data')
    parser.add_argument('--data-year', type=int, required=True, help='Data year to process')
    parser.add_argument('--edc-only', action='store_true', help='Process only EDC schools')
    parser.add_argument('--force-refresh', action='store_true', help='Reprocess existing data')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    parser.add_argument('--location-ids', type=str, help='Comma-separated location IDs to process')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing data')
    
    args = parser.parse_args()
    
    proxy_process = None
    
    try:
        print("🚀 Starting Nearby Schools Processing...")
        print(f"📅 Data Year: {args.data_year}")
        print(f"🎯 EDC Only: {args.edc_only}")
        print(f"🔄 Force Refresh: {args.force_refresh}")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Create tables if needed
        create_nearby_schools_tables(engine)
        
        # Load EDC schools if needed
        edc_schools = load_edc_schools() if args.edc_only else None
        
        if args.validate_only:
            print("\n🔍 Validating processing completeness...")
            validation = validate_processing_completeness(engine, args.data_year, edc_schools)
            if validation:
                print(f"📊 Validation Results:")
                print(f"  Total locations: {validation['total_locations']:,}")
                print(f"  Processed locations: {validation['processed_locations']:,}")
                print(f"  Processing rate: {validation['processing_rate']:.1f}%")
                print(f"  Locations with ESRI: {validation['locations_with_esri']:,}")
                print(f"  ESRI availability: {validation['esri_availability_rate']:.1f}%")
                print(f"  Unique nearby schools: {validation['unique_nearby_schools']:,}")
                print(f"  Total nearby records: {validation['total_nearby_records']:,}")
            return 0
        
        # Find locations that need processing
        if args.location_ids:
            # Process specific location IDs
            location_ids = [int(x.strip()) for x in args.location_ids.split(',')]
            print(f"🎯 Processing specific locations: {location_ids}")
            
            locations = []
            with engine.connect() as conn:
                for location_id in location_ids:
                    result = conn.execute(text("""
                        SELECT lp.id, lp.latitude, lp.longitude, 
                               COALESCE(CONCAT(sd.ncessch, '-', sd.split_suffix), sd.ncessch, sd.state_school_id) as school_id, 
                               s.uuid
                        FROM location_points lp
                        JOIN school_locations sl ON lp.id = sl.location_id
                        JOIN schools s ON sl.school_id = s.id
                        JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                        WHERE lp.id = :location_id 
                            AND sl.data_year = :data_year
                            AND sl.is_current = true
                        LIMIT 1
                    """), {'location_id': location_id, 'data_year': args.data_year}).fetchone()
                    
                    if result:
                        locations.append({
                            'location_id': result[0],
                            'latitude': float(result[1]),
                            'longitude': float(result[2]),
                            'school_id': result[3],
                            'school_uuid': result[4],
                            'data_year': args.data_year
                        })
        else:
            # Find all locations needing processing
            locations = find_locations_needing_processing(
                engine, args.data_year, edc_schools, args.force_refresh
            )
        
        if not locations:
            print("📭 No locations found that need processing")
            return 0
        
        # Process the locations
        results = process_batch_locations(engine, locations, args.data_year, args.batch_size)
        
        # Final validation
        print(f"\n🔍 Final validation...")
        validation = validate_processing_completeness(engine, args.data_year, edc_schools)
        if validation:
            print(f"📊 Final Processing Status:")
            print(f"  Processing rate: {validation['processing_rate']:.1f}%")
            print(f"  Unique nearby schools: {validation['unique_nearby_schools']:,}")
            print(f"  Total nearby records: {validation['total_nearby_records']:,}")
        
        print(f"\n✅ Processing completed!")
        print(f"  📈 Success rate: {results['success_rate']:.1f}%")
        
        # Return appropriate exit code
        if results['success_rate'] >= 95:
            return 0  # Complete success
        elif results['success_rate'] >= 50:
            return 2  # Partial success
        else:
            return 1  # Mostly failed
        
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Processing failed: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    sys.exit(main()) 