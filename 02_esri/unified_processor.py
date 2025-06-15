#!/usr/bin/env python3
"""
ESRI Data Processor for EDC Unified Database

This script fetches location coordinates from the edc_unified database,
processes ESRI demographic data, and stores results in esri_demographic_data table.
"""

import os
import sys
import subprocess
import time
import socket
import signal
import json
import logging
from datetime import datetime
from pathlib import Path

try:
    from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Text, Boolean
    from sqlalchemy.dialects.postgresql import JSONB
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy")
    sys.exit(1)

# Import the existing ESRI fetch functionality
from .fetch import fetch_esri_data

# Configuration - update these values as needed
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = './etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables for cleanup
proxy_process = None

def signal_handler(signum, frame):
    global proxy_process
    logger.info(f"Received signal {signum}. Cleaning up...")
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
    
    logger.info(f"Starting Cloud SQL Proxy on port {port}")
    
    proxy_process = subprocess.Popen(proxy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(5)
    
    if proxy_process.poll() is not None:
        _, stderr = proxy_process.communicate()
        raise Exception(f"Cloud SQL Proxy failed to start: {stderr.decode()}")
    
    logger.info("✅ Cloud SQL Proxy started successfully")
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
        logger.info("Cloud SQL Proxy stopped")

def create_connection(port):
    """Create database connection"""
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        logger.info(f"Creating connection: postgresql://{DB_USER}:***@localhost:{port}/{DB_NAME}")
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            logger.info("✅ Database connection successful!")
            return engine
            
    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        raise

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
                logger.info("Creating esri_demographic_data table...")
                
                create_table_sql = text("""
                    CREATE TABLE esri_demographic_data (
                        id SERIAL PRIMARY KEY,
                        location_id INTEGER NOT NULL,
                        latitude DECIMAL(10, 8) NOT NULL,
                        longitude DECIMAL(11, 8) NOT NULL,
                        drive_time INTEGER NOT NULL,
                        
                        -- Raw demographic data
                        age4_cy DECIMAL,
                        age5_cy DECIMAL,
                        age6_cy DECIMAL,
                        age7_cy DECIMAL,
                        age8_cy DECIMAL,
                        age9_cy DECIMAL,
                        age10_cy DECIMAL,
                        age11_cy DECIMAL,
                        age12_cy DECIMAL,
                        age13_cy DECIMAL,
                        age14_cy DECIMAL,
                        age15_cy DECIMAL,
                        age16_cy DECIMAL,
                        age17_cy DECIMAL,
                        
                        -- Future year data
                        age4_fy DECIMAL,
                        age5_fy DECIMAL,
                        age6_fy DECIMAL,
                        age7_fy DECIMAL,
                        age8_fy DECIMAL,
                        age9_fy DECIMAL,
                        age10_fy DECIMAL,
                        age11_fy DECIMAL,
                        age12_fy DECIMAL,
                        age13_fy DECIMAL,
                        age14_fy DECIMAL,
                        age15_fy DECIMAL,
                        age16_fy DECIMAL,
                        age17_fy DECIMAL,
                        
                        -- 2020 Census data
                        age4_c20 DECIMAL,
                        age5_c20 DECIMAL,
                        age6_c20 DECIMAL,
                        age7_c20 DECIMAL,
                        age8_c20 DECIMAL,
                        age9_c20 DECIMAL,
                        age10_c20 DECIMAL,
                        age11_c20 DECIMAL,
                        age12_c20 DECIMAL,
                        age13_c20 DECIMAL,
                        age14_c20 DECIMAL,
                        age15_c20 DECIMAL,
                        age16_c20 DECIMAL,
                        age17_c20 DECIMAL,
                        
                        -- Adult demographic percentages (2020)
                        per_hisp_adult_20 DECIMAL,
                        per_wht_adult_20 DECIMAL,
                        per_blk_adult_20 DECIMAL,
                        per_asn_adult_20 DECIMAL,
                        per_pi_adult_20 DECIMAL,
                        per_ai_adult_20 DECIMAL,
                        per_other_adult_20 DECIMAL,
                        per_two_or_more_adult_20 DECIMAL,
                        
                        -- Child demographic percentages (2020)
                        per_hisp_child_20 DECIMAL,
                        per_wht_child_20 DECIMAL,
                        per_blk_child_20 DECIMAL,
                        per_asn_child_20 DECIMAL,
                        per_pi_child_20 DECIMAL,
                        per_ai_child_20 DECIMAL,
                        per_other_child_20 DECIMAL,
                        per_two_or_more_child_20 DECIMAL,
                        
                        -- Economic data
                        medhinc_cy DECIMAL,
                        per_50k_cy DECIMAL,
                        per_renter_cy DECIMAL,
                        per_vacant_cy DECIMAL,
                        
                        -- Polygon data
                        drive_time_polygon JSONB,
                        
                        -- Metadata
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        has_data BOOLEAN DEFAULT TRUE,
                        
                        -- Constraints
                        UNIQUE(location_id, drive_time)
                    );
                    
                    CREATE INDEX idx_esri_demographic_location_id ON esri_demographic_data(location_id);
                    CREATE INDEX idx_esri_demographic_drive_time ON esri_demographic_data(drive_time);
                    CREATE INDEX idx_esri_demographic_processed_at ON esri_demographic_data(processed_at);
                """)
                
                conn.execute(create_table_sql)
                conn.commit()
                logger.info("✅ esri_demographic_data table created successfully")
            else:
                logger.info("esri_demographic_data table already exists")
                
    except Exception as e:
        logger.error(f"Error creating demographic table: {str(e)}")
        raise

def get_location_coordinates(engine, location_id):
    """Get latitude and longitude for a specific location_id"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT latitude, longitude 
                FROM location_points 
                WHERE location_id = :location_id
            """)
            
            result = conn.execute(query, {'location_id': location_id}).fetchone()
            
            if result:
                lat, lon = result
                logger.info(f"Found coordinates for location_id {location_id}: {lat}, {lon}")
                return float(lat), float(lon)
            else:
                logger.error(f"No coordinates found for location_id {location_id}")
                return None, None
                
    except Exception as e:
        logger.error(f"Error fetching coordinates for location_id {location_id}: {str(e)}")
        return None, None

def check_existing_data(engine, location_id):
    """Check if ESRI data already exists for this location"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT COUNT(*) 
                FROM esri_demographic_data 
                WHERE location_id = :location_id
                AND processed_at >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            count = conn.execute(query, {'location_id': location_id}).scalar()
            return count > 0
            
    except Exception as e:
        logger.error(f"Error checking existing data: {str(e)}")
        return False

def store_esri_data(engine, location_id, latitude, longitude, esri_data):
    """Store ESRI demographic data in the database"""
    try:
        with engine.begin() as conn:
            # Delete existing data for this location first
            delete_query = text("""
                DELETE FROM esri_demographic_data 
                WHERE location_id = :location_id
            """)
            conn.execute(delete_query, {'location_id': location_id})
            
            stored_count = 0
            for drive_time, data in esri_data.items():
                try:
                    # Prepare the data for insertion
                    insert_data = {
                        'location_id': location_id,
                        'latitude': latitude,
                        'longitude': longitude,
                        'drive_time': drive_time,
                        'processed_at': datetime.utcnow(),
                        'has_data': True
                    }
                    
                    # Add all the demographic data fields
                    demographic_fields = [
                        'age4_cy', 'age5_cy', 'age6_cy', 'age7_cy', 'age8_cy', 'age9_cy', 'age10_cy',
                        'age11_cy', 'age12_cy', 'age13_cy', 'age14_cy', 'age15_cy', 'age16_cy', 'age17_cy',
                        'age4_fy', 'age5_fy', 'age6_fy', 'age7_fy', 'age8_fy', 'age9_fy', 'age10_fy',
                        'age11_fy', 'age12_fy', 'age13_fy', 'age14_fy', 'age15_fy', 'age16_fy', 'age17_fy',
                        'age4_c20', 'age5_c20', 'age6_c20', 'age7_c20', 'age8_c20', 'age9_c20', 'age10_c20',
                        'age11_c20', 'age12_c20', 'age13_c20', 'age14_c20', 'age15_c20', 'age16_c20', 'age17_c20',
                        'per_hisp_adult_20', 'per_wht_adult_20', 'per_blk_adult_20', 'per_asn_adult_20',
                        'per_pi_adult_20', 'per_ai_adult_20', 'per_other_adult_20', 'per_two_or_more_adult_20',
                        'per_hisp_child_20', 'per_wht_child_20', 'per_blk_child_20', 'per_asn_child_20',
                        'per_pi_child_20', 'per_ai_child_20', 'per_other_child_20', 'per_two_or_more_child_20',
                        'medhinc_cy', 'per_50k_cy', 'per_renter_cy', 'per_vacant_cy'
                    ]
                    
                    # Add demographic data with safe conversion
                    for field in demographic_fields:
                        value = data.get(field)
                        if value is not None:
                            try:
                                insert_data[field] = float(value)
                            except (ValueError, TypeError):
                                insert_data[field] = None
                        else:
                            insert_data[field] = None
                    
                    # Add polygon data
                    polygon_data = data.get('drive_time_polygon')
                    if polygon_data:
                        if isinstance(polygon_data, str):
                            insert_data['drive_time_polygon'] = polygon_data
                        else:
                            insert_data['drive_time_polygon'] = json.dumps(polygon_data)
                    
                    # Build the INSERT query dynamically
                    columns = list(insert_data.keys())
                    placeholders = [f':{col}' for col in columns]
                    
                    insert_query = text(f"""
                        INSERT INTO esri_demographic_data ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                    """)
                    
                    conn.execute(insert_query, insert_data)
                    stored_count += 1
                    logger.info(f"✅ Stored data for drive time {drive_time} minutes")
                    
                except Exception as e:
                    logger.error(f"Error storing data for drive time {drive_time}: {str(e)}")
                    raise
            
            logger.info(f"✅ Successfully stored {stored_count} demographic records for location_id {location_id}")
            return stored_count
            
    except Exception as e:
        logger.error(f"Error storing ESRI data: {str(e)}")
        raise

def process_location(engine, location_id, force_refresh=False):
    """Process ESRI data for a specific location_id"""
    try:
        logger.info(f"🚀 Starting ESRI processing for location_id: {location_id}")
        
        # Check if data already exists and is recent
        if not force_refresh and check_existing_data(engine, location_id):
            logger.info(f"Recent ESRI data already exists for location_id {location_id}. Use force_refresh=True to override.")
            return True
        
        # Get coordinates
        latitude, longitude = get_location_coordinates(engine, location_id)
        if latitude is None or longitude is None:
            logger.error(f"Could not get coordinates for location_id {location_id}")
            return False
        
        # Fetch ESRI data
        logger.info(f"📡 Fetching ESRI data for coordinates: {latitude}, {longitude}")
        esri_data = fetch_esri_data(latitude, longitude)
        
        if not esri_data:
            logger.error("Failed to fetch ESRI data")
            return False
        
        logger.info(f"✅ Successfully fetched ESRI data for {len(esri_data)} drive times")
        
        # Store data in database
        stored_count = store_esri_data(engine, location_id, latitude, longitude, esri_data)
        
        if stored_count > 0:
            logger.info(f"🎉 Successfully processed location_id {location_id} with {stored_count} records")
            return True
        else:
            logger.error(f"Failed to store any data for location_id {location_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing location_id {location_id}: {str(e)}")
        return False

def process_multiple_locations(engine, location_ids, force_refresh=False):
    """Process ESRI data for multiple location_ids"""
    success_count = 0
    total_count = len(location_ids)
    
    logger.info(f"🚀 Starting batch processing for {total_count} locations")
    
    for i, location_id in enumerate(location_ids, 1):
        logger.info(f"Processing location {i}/{total_count}: {location_id}")
        
        try:
            if process_location(engine, location_id, force_refresh):
                success_count += 1
                logger.info(f"✅ Successfully processed location_id {location_id} ({i}/{total_count})")
            else:
                logger.error(f"❌ Failed to process location_id {location_id} ({i}/{total_count})")
                
        except Exception as e:
            logger.error(f"❌ Error processing location_id {location_id}: {str(e)}")
        
        # Add a small delay between requests to be respectful to the ESRI API
        if i < total_count:
            time.sleep(2)
    
    logger.info(f"🎉 Batch processing complete: {success_count}/{total_count} locations processed successfully")
    return success_count

def main():
    global proxy_process
    
    try:
        logger.info("🔍 Starting ESRI unified processor...")
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logger.error(f"⚠️  Service account file not found: {SERVICE_ACCOUNT_FILE}")
            logger.error("Please ensure the service account key file is in the correct location.")
            return 1
        
        # Start Cloud SQL Proxy
        logger.info("🚀 Starting Cloud SQL Proxy...")
        proxy_process, port = start_cloud_sql_proxy()
        
        # Create connection
        engine = create_connection(port)
        
        # Create the demographic table if needed
        create_demographic_table(engine)
        
        # Example usage - you can modify this section
        # Process a single location
        example_location_id = 1  # Change this to your desired location_id
        success = process_location(engine, example_location_id, force_refresh=True)
        
        if success:
            logger.info("✅ Processing completed successfully!")
            return 0
        else:
            logger.error("❌ Processing failed!")
            return 1
        
    except KeyboardInterrupt:
        logger.error("❌ Process interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup
        if proxy_process:
            logger.info("Stopping Cloud SQL Proxy...")
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    print("="*60)
    print("ESRI UNIFIED PROCESSOR")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result) 