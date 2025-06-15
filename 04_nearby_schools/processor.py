#!/usr/bin/env python3
"""
Nearby Schools Processor for EDC Unified Database

This processor uses ESRI drive-time polygons to identify schools within catchment areas
and populates the school_polygon_relationships and nearby_school_polygons tables.
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
from typing import List, Dict, Tuple, Optional

try:
    from sqlalchemy import create_engine, text
    from shapely.geometry import Point, Polygon
    from shapely.ops import transform
    import shapely.wkt
except ImportError:
    print("Required packages not found. Please install:")
    print("pip install sqlalchemy psycopg2-binary shapely")
    sys.exit(1)

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

def get_schools_with_locations(engine, data_year: int) -> List[Dict]:
    """Get all schools with their current locations for a specific data year"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    s.id as school_id,
                    s.uuid as school_uuid,
                    sl.location_id,
                    lp.latitude,
                    lp.longitude,
                    sl.data_year,
                    sl.school_year
                FROM schools s
                JOIN school_locations sl ON s.id = sl.school_id
                JOIN location_points lp ON sl.location_id = lp.id
                WHERE sl.data_year = :data_year
                    AND lp.latitude IS NOT NULL 
                    AND lp.longitude IS NOT NULL
                    AND sl.is_current = true
                ORDER BY s.id
            """)
            
            results = conn.execute(query, {'data_year': data_year}).fetchall()
            
            schools = []
            for row in results:
                schools.append({
                    'school_id': row[0],
                    'school_uuid': row[1],
                    'location_id': row[2],
                    'latitude': float(row[3]),
                    'longitude': float(row[4]),
                    'data_year': row[5],
                    'school_year': row[6]
                })
            
            logger.info(f"Found {len(schools)} schools with locations for data year {data_year}")
            return schools
            
    except Exception as e:
        logger.error(f"Error getting schools with locations: {str(e)}")
        raise

def get_esri_polygons(engine, location_id: int) -> Dict[int, Dict]:
    """Get ESRI drive-time polygons for a location"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT drive_time, drive_time_polygon, latitude, longitude
                FROM esri_demographic_data
                WHERE location_id = :location_id
                    AND drive_time_polygon IS NOT NULL
                ORDER BY drive_time
            """)
            
            results = conn.execute(query, {'location_id': location_id}).fetchall()
            
            polygons = {}
            for row in results:
                drive_time = row[0]
                polygon_json = row[1]
                latitude = row[2]
                longitude = row[3]
                
                if polygon_json:
                    try:
                        # Parse the polygon JSON
                        if isinstance(polygon_json, str):
                            polygon_data = json.loads(polygon_json)
                        else:
                            polygon_data = polygon_json
                        
                        # Extract the rings from ESRI format
                        if 'rings' in polygon_data and polygon_data['rings']:
                            rings = polygon_data['rings'][0]  # Use first ring
                            if rings:
                                # Create Shapely polygon
                                polygon = Polygon(rings)
                                if polygon.is_valid:
                                    polygons[drive_time] = {
                                        'polygon': polygon,
                                        'polygon_json': polygon_json,
                                        'latitude': latitude,
                                        'longitude': longitude
                                    }
                                else:
                                    logger.warning(f"Invalid polygon geometry for location {location_id}, drive time {drive_time}")
                            else:
                                logger.warning(f"Empty rings for location {location_id}, drive time {drive_time}")
                        else:
                            logger.warning(f"No rings found in polygon data for location {location_id}, drive time {drive_time}")
                            
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.error(f"Error parsing polygon for location {location_id}, drive time {drive_time}: {str(e)}")
                        continue
            
            logger.debug(f"Found {len(polygons)} valid polygons for location {location_id}")
            return polygons
            
    except Exception as e:
        logger.error(f"Error getting ESRI polygons for location {location_id}: {str(e)}")
        return {}

def find_nearby_schools(target_location_id: int, target_polygons: Dict, all_schools: List[Dict]) -> Dict[int, List[Dict]]:
    """Find schools within each drive-time polygon"""
    nearby_schools = {}
    
    for drive_time, polygon_data in target_polygons.items():
        polygon = polygon_data['polygon']
        schools_in_polygon = []
        
        for school in all_schools:
            # Skip the target school's own location
            if school['location_id'] == target_location_id:
                continue
                
            # Create point for school location
            school_point = Point(school['longitude'], school['latitude'])
            
            # Check if school is within the drive-time polygon
            if polygon.contains(school_point):
                schools_in_polygon.append({
                    'school_id': school['school_id'],
                    'school_uuid': school['school_uuid'],
                    'location_id': school['location_id'],
                    'latitude': school['latitude'],
                    'longitude': school['longitude'],
                    'data_year': school['data_year']
                })
        
        nearby_schools[drive_time] = schools_in_polygon
        logger.debug(f"Found {len(schools_in_polygon)} schools within {drive_time}-minute drive time")
    
    return nearby_schools

def store_polygon_relationships(engine, location_id: int, data_year: int, nearby_schools: Dict[int, List[Dict]]) -> bool:
    """Store school polygon relationships and nearby school data"""
    try:
        with engine.begin() as conn:
            # First, clean up existing data for this location and year
            cleanup_existing = text("""
                DELETE FROM nearby_school_polygons 
                WHERE polygon_relationship_id IN (
                    SELECT id FROM school_polygon_relationships 
                    WHERE location_id = :location_id AND data_year = :data_year
                )
            """)
            conn.execute(cleanup_existing, {
                'location_id': location_id, 
                'data_year': data_year
            })
            
            cleanup_relationships = text("""
                DELETE FROM school_polygon_relationships 
                WHERE location_id = :location_id AND data_year = :data_year
            """)
            conn.execute(cleanup_relationships, {
                'location_id': location_id, 
                'data_year': data_year
            })
            
            relationships_created = 0
            nearby_schools_created = 0
            
            for drive_time, schools_list in nearby_schools.items():
                # Create school_polygon_relationship record
                insert_relationship = text("""
                    INSERT INTO school_polygon_relationships 
                    (location_id, drive_time, data_year, processed_at, created_at, updated_at)
                    VALUES (:location_id, :drive_time, :data_year, :processed_at, :created_at, :updated_at)
                    RETURNING id
                """)
                
                now = datetime.utcnow()
                result = conn.execute(insert_relationship, {
                    'location_id': location_id,
                    'drive_time': drive_time,
                    'data_year': data_year,
                    'processed_at': now,
                    'created_at': now,
                    'updated_at': now
                })
                
                relationship_id = result.scalar()
                relationships_created += 1
                
                # Create nearby_school_polygons records for each school in this polygon
                for school in schools_list:
                    insert_nearby = text("""
                        INSERT INTO nearby_school_polygons 
                        (polygon_relationship_id, school_uuid, relationship_type, created_at)
                        VALUES (:polygon_relationship_id, :school_uuid, :relationship_type, :created_at)
                    """)
                    
                    conn.execute(insert_nearby, {
                        'polygon_relationship_id': relationship_id,
                        'school_uuid': school['school_uuid'],
                        'relationship_type': 'nearby',  # Using 'nearby' as the standard relationship type
                        'created_at': now
                    })
                    
                    nearby_schools_created += 1
                
                logger.debug(f"Created relationship for drive time {drive_time} with {len(schools_list)} nearby schools")
            
            logger.info(f"✅ Created {relationships_created} polygon relationships and {nearby_schools_created} nearby school records for location {location_id}")
            return True
            
    except Exception as e:
        logger.error(f"Error storing polygon relationships for location {location_id}: {str(e)}")
        raise

def process_location(engine, location_id: int, data_year: int = None, force_refresh: bool = False) -> bool:
    """Process nearby schools for a specific location"""
    if data_year is None:
        data_year = datetime.now().year
    
    try:
        logger.info(f"🚀 Processing nearby schools for location_id: {location_id}, data_year: {data_year}")
        
        # Check if processing is needed
        if not force_refresh:
            with engine.connect() as conn:
                existing_check = text("""
                    SELECT COUNT(*) FROM school_polygon_relationships 
                    WHERE location_id = :location_id AND data_year = :data_year
                """)
                existing_count = conn.execute(existing_check, {
                    'location_id': location_id, 
                    'data_year': data_year
                }).scalar()
                
                if existing_count > 0:
                    logger.info(f"Nearby schools data already exists for location {location_id}, year {data_year}. Use force_refresh=True to override.")
                    return True
        
        # Get all schools with locations for this data year
        logger.info(f"📍 Fetching schools with locations for data year {data_year}")
        all_schools = get_schools_with_locations(engine, data_year)
        
        if not all_schools:
            logger.error(f"No schools with locations found for data year {data_year}")
            return False
        
        # Get ESRI polygons for the target location
        logger.info(f"🗺️  Fetching ESRI polygons for location {location_id}")
        target_polygons = get_esri_polygons(engine, location_id)
        
        if not target_polygons:
            logger.error(f"No ESRI polygons found for location {location_id}. Run ESRI processing first.")
            return False
        
        logger.info(f"Found polygons for drive times: {list(target_polygons.keys())}")
        
        # Find nearby schools within each polygon
        logger.info(f"🔍 Finding nearby schools within drive-time polygons")
        nearby_schools = find_nearby_schools(location_id, target_polygons, all_schools)
        
        # Log summary
        total_nearby = sum(len(schools) for schools in nearby_schools.values())
        logger.info(f"Found {total_nearby} total nearby school relationships:")
        for drive_time, schools in nearby_schools.items():
            logger.info(f"  {drive_time} minutes: {len(schools)} schools")
        
        # Store the results
        logger.info(f"💾 Storing polygon relationships and nearby schools")
        success = store_polygon_relationships(engine, location_id, data_year, nearby_schools)
        
        if success:
            logger.info(f"🎉 Successfully processed nearby schools for location_id {location_id}")
            return True
        else:
            logger.error(f"Failed to store nearby schools for location_id {location_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing location {location_id}: {str(e)}")
        return False

def process_multiple_locations(engine, location_ids: List[int], data_year: int = None, force_refresh: bool = False) -> int:
    """Process nearby schools for multiple locations"""
    if data_year is None:
        data_year = datetime.now().year
    
    success_count = 0
    total_count = len(location_ids)
    
    logger.info(f"🚀 Starting batch nearby schools processing for {total_count} locations, data year {data_year}")
    
    for i, location_id in enumerate(location_ids, 1):
        logger.info(f"Processing location {i}/{total_count}: {location_id}")
        
        try:
            if process_location(engine, location_id, data_year, force_refresh):
                success_count += 1
                logger.info(f"✅ Successfully processed location_id {location_id} ({i}/{total_count})")
            else:
                logger.error(f"❌ Failed to process location_id {location_id} ({i}/{total_count})")
                
        except Exception as e:
            logger.error(f"❌ Error processing location_id {location_id}: {str(e)}")
        
        # Small delay between processing
        if i < total_count:
            time.sleep(1)
    
    logger.info(f"🎉 Batch processing complete: {success_count}/{total_count} locations processed successfully")
    return success_count

def get_processing_summary(engine, data_year: int = None) -> Dict:
    """Get summary of nearby schools processing"""
    if data_year is None:
        data_year = datetime.now().year
    
    try:
        with engine.connect() as conn:
            # Get polygon relationships summary
            relationships_query = text("""
                SELECT 
                    COUNT(*) as total_relationships,
                    COUNT(DISTINCT location_id) as unique_locations,
                    COUNT(DISTINCT drive_time) as drive_times,
                    MIN(processed_at) as earliest_processed,
                    MAX(processed_at) as latest_processed
                FROM school_polygon_relationships
                WHERE data_year = :data_year
            """)
            
            relationships_result = conn.execute(relationships_query, {'data_year': data_year}).fetchone()
            
            # Get nearby schools summary
            nearby_query = text("""
                SELECT 
                    COUNT(*) as total_nearby_schools,
                    COUNT(DISTINCT school_uuid) as unique_schools
                FROM nearby_school_polygons nsp
                JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.data_year = :data_year
            """)
            
            nearby_result = conn.execute(nearby_query, {'data_year': data_year}).fetchone()
            
            # Get drive time breakdown
            drive_time_query = text("""
                SELECT 
                    spr.drive_time,
                    COUNT(*) as polygon_count,
                    COUNT(nsp.id) as nearby_school_count
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                WHERE spr.data_year = :data_year
                GROUP BY spr.drive_time
                ORDER BY spr.drive_time
            """)
            
            drive_time_results = conn.execute(drive_time_query, {'data_year': data_year}).fetchall()
            
            return {
                'data_year': data_year,
                'total_relationships': relationships_result[0] if relationships_result else 0,
                'unique_locations': relationships_result[1] if relationships_result else 0,
                'drive_times_processed': relationships_result[2] if relationships_result else 0,
                'earliest_processed': relationships_result[3] if relationships_result else None,
                'latest_processed': relationships_result[4] if relationships_result else None,
                'total_nearby_schools': nearby_result[0] if nearby_result else 0,
                'unique_schools': nearby_result[1] if nearby_result else 0,
                'drive_time_breakdown': [
                    {
                        'drive_time': row[0],
                        'polygon_count': row[1],
                        'nearby_school_count': row[2]
                    }
                    for row in drive_time_results
                ]
            }
            
    except Exception as e:
        logger.error(f"Error getting processing summary: {str(e)}")
        return {}

def main():
    global proxy_process
    
    try:
        logger.info("🔍 Starting nearby schools processor...")
        
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
        
        # Example usage - process a single location
        example_location_id = 1  # Change this to your desired location_id
        data_year = datetime.now().year
        
        success = process_location(engine, example_location_id, data_year, force_refresh=True)
        
        # Show summary
        summary = get_processing_summary(engine, data_year)
        if summary:
            logger.info("📊 Processing Summary:")
            logger.info(f"  Total relationships: {summary['total_relationships']}")
            logger.info(f"  Unique locations: {summary['unique_locations']}")
            logger.info(f"  Total nearby schools: {summary['total_nearby_schools']}")
            
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
    print("NEARBY SCHOOLS PROCESSOR")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result) 