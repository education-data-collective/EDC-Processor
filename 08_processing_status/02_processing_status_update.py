#!/usr/bin/env python3
"""
Processing Status Update Script

This script updates the processing_status table by checking actual data presence
across related tables to determine the true/false state of each processing flag.

Updated Logic (based on user feedback):
1. enrollment_processed: Both school_enrollments AND school_grades_offered
2. location_processed: school_locations has valid records  
3. characteristics_processed: school_characteristics has records
4. projections_processed: school_projections has records
5. demographics_processed: ALL THREE: esri_demographic_data, school_polygon_relationships, AND nearby_school_polygons
6. nces_processed: Complete NCES data (directory, names, etc.)
7. geocoding_processed: ALL fields in location_points are populated
8. esri_processed: Recent esri_demographic_data records
9. district_metrics_processed: school_metrics has records
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

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy")
    sys.exit(1)

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = '../etl-service-account-key.json'
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

def check_enrollment_processed(conn, school_id, data_year):
    """Check if enrollment is processed: Both school_enrollments AND school_grades_offered"""
    try:
        # Check school_enrollments
        enrollment_result = conn.execute(text("""
            SELECT COUNT(*) FROM school_enrollments 
            WHERE school_id = :school_id AND data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        # Check school_grades_offered
        grades_result = conn.execute(text("""
            SELECT COUNT(*) FROM school_grades_offered 
            WHERE school_id = :school_id AND data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return enrollment_result > 0 and grades_result > 0
        
    except Exception as e:
        print(f"❌ Error checking enrollment for school {school_id}: {str(e)}")
        return False

def check_location_processed(conn, school_id, data_year):
    """Check if location is processed: school_locations has valid records"""
    try:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM school_locations sl
            JOIN location_points lp ON sl.location_id = lp.id
            WHERE sl.school_id = :school_id AND sl.data_year = :data_year
            AND lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking location for school {school_id}: {str(e)}")
        return False

def check_characteristics_processed(conn, school_id, data_year):
    """Check if characteristics are processed: school_characteristics has records"""
    try:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM school_characteristics 
            WHERE school_id = :school_id AND data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking characteristics for school {school_id}: {str(e)}")
        return False

def check_projections_processed(conn, school_id, data_year):
    """Check if projections are processed: school_projections has records"""
    try:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM school_projections 
            WHERE school_id = :school_id AND data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking projections for school {school_id}: {str(e)}")
        return False

def check_demographics_processed(conn, school_id, data_year):
    """Check demographics: ALL THREE tables - esri_demographic_data, school_polygon_relationships, nearby_school_polygons"""
    try:
        # Get school's location
        location_result = conn.execute(text("""
            SELECT sl.location_id FROM school_locations sl
            WHERE sl.school_id = :school_id AND sl.data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).fetchone()
        
        if not location_result:
            return False
        
        location_id = location_result[0]
        
        # Check esri_demographic_data
        esri_result = conn.execute(text("""
            SELECT COUNT(*) FROM esri_demographic_data 
            WHERE location_id = :location_id
        """), {'location_id': location_id}).scalar()
        
        # Check school_polygon_relationships
        polygon_result = conn.execute(text("""
            SELECT COUNT(*) FROM school_polygon_relationships 
            WHERE location_id = :location_id AND data_year = :data_year
        """), {'location_id': location_id, 'data_year': data_year}).scalar()
        
        # Check nearby_school_polygons (via school_polygon_relationships)
        nearby_result = conn.execute(text("""
            SELECT COUNT(*) FROM nearby_school_polygons nsp
            JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
            WHERE spr.location_id = :location_id AND spr.data_year = :data_year
        """), {'location_id': location_id, 'data_year': data_year}).scalar()
        
        return esri_result > 0 and polygon_result > 0 and nearby_result > 0
        
    except Exception as e:
        print(f"❌ Error checking demographics for school {school_id}: {str(e)}")
        return False

def check_nces_processed(conn, school_id, data_year):
    """Check NCES processing: Complete NCES data (directory, names, etc.)"""
    try:
        # Check school_directory
        directory_result = conn.execute(text("""
            SELECT COUNT(*) FROM school_directory 
            WHERE school_id = :school_id AND data_year = :data_year
            AND ncessch IS NOT NULL AND system_name IS NOT NULL
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        # Check school_names
        names_result = conn.execute(text("""
            SELECT COUNT(*) FROM school_names 
            WHERE school_id = :school_id AND data_year = :data_year 
            AND is_active = true AND display_name IS NOT NULL
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return directory_result > 0 and names_result > 0
        
    except Exception as e:
        print(f"❌ Error checking NCES for school {school_id}: {str(e)}")
        return False

def check_geocoding_processed(conn, school_id, data_year):
    """Check geocoding: ALL fields in location_points are populated"""
    try:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM school_locations sl
            JOIN location_points lp ON sl.location_id = lp.id
            WHERE sl.school_id = :school_id AND sl.data_year = :data_year
            AND lp.latitude IS NOT NULL 
            AND lp.longitude IS NOT NULL
            AND lp.address IS NOT NULL 
            AND lp.city IS NOT NULL
            AND lp.county IS NOT NULL
            AND lp.state IS NOT NULL
            AND lp.zip_code IS NOT NULL
        """), {'school_id': school_id, 'data_year': data_year}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking geocoding for school {school_id}: {str(e)}")
        return False

def check_esri_processed(conn, school_id, data_year):
    """Check ESRI processing: Recent esri_demographic_data records"""
    try:
        # Get school's location
        location_result = conn.execute(text("""
            SELECT sl.location_id FROM school_locations sl
            WHERE sl.school_id = :school_id AND sl.data_year = :data_year
        """), {'school_id': school_id, 'data_year': data_year}).fetchone()
        
        if not location_result:
            return False
        
        location_id = location_result[0]
        
        # Check for recent ESRI data (within last year)
        cutoff_date = datetime.now() - timedelta(days=365)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM esri_demographic_data 
            WHERE location_id = :location_id 
            AND processed_at >= :cutoff_date
        """), {'location_id': location_id, 'cutoff_date': cutoff_date}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking ESRI for school {school_id}: {str(e)}")
        return False

def check_district_metrics_processed(conn, school_id, data_year):
    """Check district metrics: school_metrics has records"""
    try:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM school_metrics 
            WHERE school_id = :school_id
        """), {'school_id': school_id}).scalar()
        
        return result > 0
        
    except Exception as e:
        print(f"❌ Error checking district metrics for school {school_id}: {str(e)}")
        return False

def determine_data_completeness(flags):
    """Determine data completeness based on processing flags"""
    total_flags = len(flags)
    true_flags = sum(1 for flag in flags.values() if flag)
    
    if true_flags == total_flags:
        return 'complete'
    elif true_flags > 0:
        return 'partial'
    else:
        return 'none'

def update_processing_status_record(conn, school_id, data_year):
    """Update a single processing status record"""
    try:
        # Check all processing flags
        flags = {
            'enrollment_processed': check_enrollment_processed(conn, school_id, data_year),
            'location_processed': check_location_processed(conn, school_id, data_year),
            'characteristics_processed': check_characteristics_processed(conn, school_id, data_year),
            'projections_processed': check_projections_processed(conn, school_id, data_year),
            'demographics_processed': check_demographics_processed(conn, school_id, data_year),
            'nces_processed': check_nces_processed(conn, school_id, data_year),
            'geocoding_processed': check_geocoding_processed(conn, school_id, data_year),
            'esri_processed': check_esri_processed(conn, school_id, data_year),
            'district_metrics_processed': check_district_metrics_processed(conn, school_id, data_year),
        }
        
        # Determine data completeness
        data_completeness = determine_data_completeness(flags)
        
        # Update the record
        update_query = text("""
            UPDATE processing_status SET
                enrollment_processed = :enrollment_processed,
                location_processed = :location_processed,
                characteristics_processed = :characteristics_processed,
                projections_processed = :projections_processed,
                demographics_processed = :demographics_processed,
                nces_processed = :nces_processed,
                geocoding_processed = :geocoding_processed,
                esri_processed = :esri_processed,
                district_metrics_processed = :district_metrics_processed,
                data_completeness = :data_completeness,
                last_processed_at = :now,
                updated_at = :now
            WHERE school_id = :school_id AND data_year = :data_year
        """)
        
        conn.execute(update_query, {
            **flags,
            'data_completeness': data_completeness,
            'school_id': school_id,
            'data_year': data_year,
            'now': datetime.now()
        })
        
        return flags, data_completeness
        
    except Exception as e:
        print(f"❌ Error updating processing status for school {school_id}, year {data_year}: {str(e)}")
        return None, None

def create_missing_processing_status_records(engine):
    """Create processing_status records for schools that don't have them"""
    try:
        with engine.connect() as conn:
            # Find schools without processing status records
            missing_query = text("""
                SELECT DISTINCT s.id as school_id, sd.data_year
                FROM schools s
                JOIN school_directory sd ON s.id = sd.school_id
                LEFT JOIN processing_status ps ON s.id = ps.school_id AND sd.data_year = ps.data_year
                WHERE ps.id IS NULL
                ORDER BY s.id, sd.data_year
            """)
            
            missing_records = conn.execute(missing_query).fetchall()
            
            if missing_records:
                print(f"📝 Creating {len(missing_records)} missing processing status records...")
                
                for record in missing_records:
                    school_id, data_year = record
                    
                    insert_query = text("""
                        INSERT INTO processing_status (
                            school_id, data_year, enrollment_processed, location_processed,
                            characteristics_processed, projections_processed, demographics_processed,
                            nces_processed, geocoding_processed, esri_processed, district_metrics_processed,
                            data_completeness, created_at, updated_at
                        ) VALUES (
                            :school_id, :data_year, false, false, false, false, false,
                            false, false, false, false, 'none', :now, :now
                        )
                    """)
                    
                    conn.execute(insert_query, {
                        'school_id': school_id,
                        'data_year': data_year,
                        'now': datetime.now()
                    })
                
                conn.commit()
                print(f"✅ Created {len(missing_records)} missing processing status records")
            else:
                print("ℹ️  All schools already have processing status records")
                
    except Exception as e:
        print(f"❌ Error creating missing processing status records: {str(e)}")

def update_all_processing_status(engine, limit=None):
    """Update all processing status records"""
    try:
        with engine.begin() as conn:  # Use transaction
            # Get all processing status records
            query = text("""
                SELECT school_id, data_year 
                FROM processing_status 
                ORDER BY school_id, data_year
            """)
            
            if limit:
                query = text(f"""
                    SELECT school_id, data_year 
                    FROM processing_status 
                    ORDER BY school_id, data_year
                    LIMIT {limit}
                """)
            
            records = conn.execute(query).fetchall()
            
            if not records:
                print("📭 No processing status records found to update")
                return 0
            
            print(f"🔄 Updating {len(records)} processing status records...")
            
            updated_count = 0
            error_count = 0
            
            for i, (school_id, data_year) in enumerate(records, 1):
                try:
                    if i % 50 == 0:  # Progress update every 50 records
                        print(f"  📊 Progress: {i}/{len(records)} ({(i/len(records)*100):.1f}%)")
                    
                    flags, data_completeness = update_processing_status_record(conn, school_id, data_year)
                    
                    if flags is not None:
                        updated_count += 1
                        
                        # Log interesting cases
                        if data_completeness == 'complete':
                            pass  # Don't log every complete record
                        elif sum(flags.values()) == 0:
                            print(f"  ⚠️  School {school_id} ({data_year}): No processing completed")
                        
                    else:
                        error_count += 1
                        
                except Exception as e:
                    print(f"❌ Error processing school {school_id}, year {data_year}: {str(e)}")
                    error_count += 1
                    continue
            
            print(f"✅ Processing completed!")
            print(f"  • Updated: {updated_count} records")
            print(f"  • Errors: {error_count} records")
            
            return updated_count
            
    except Exception as e:
        print(f"❌ Error updating processing status records: {str(e)}")
        return 0

def get_update_summary(engine):
    """Get summary after update"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    SUM(CASE WHEN enrollment_processed THEN 1 ELSE 0 END) as enrollment_count,
                    SUM(CASE WHEN location_processed THEN 1 ELSE 0 END) as location_count,
                    SUM(CASE WHEN demographics_processed THEN 1 ELSE 0 END) as demographics_count,
                    SUM(CASE WHEN data_completeness = 'complete' THEN 1 ELSE 0 END) as complete_count,
                    SUM(CASE WHEN data_completeness = 'partial' THEN 1 ELSE 0 END) as partial_count,
                    SUM(CASE WHEN data_completeness = 'none' THEN 1 ELSE 0 END) as none_count
                FROM processing_status
            """)).fetchone()
            
            if result and result[0] > 0:
                print(f"\n📊 UPDATE SUMMARY:")
                print(f"  • Total records: {result[0]:,}")
                print(f"  • Enrollment processed: {result[1]:,} ({(result[1]/result[0]*100):.1f}%)")
                print(f"  • Location processed: {result[2]:,} ({(result[2]/result[0]*100):.1f}%)")
                print(f"  • Demographics processed: {result[3]:,} ({(result[3]/result[0]*100):.1f}%)")
                print(f"  • Complete: {result[4]:,} ({(result[4]/result[0]*100):.1f}%)")
                print(f"  • Partial: {result[5]:,} ({(result[5]/result[0]*100):.1f}%)")
                print(f"  • None: {result[6]:,} ({(result[6]/result[0]*100):.1f}%)")
                
    except Exception as e:
        print(f"❌ Error getting update summary: {str(e)}")

def main():
    global proxy_process
    
    parser = argparse.ArgumentParser(description='Update processing status records')
    parser.add_argument('--limit', type=int, help='Limit number of records to update (for testing)')
    parser.add_argument('--create-missing', action='store_true', help='Create missing processing status records')
    parser.add_argument('--update-existing', action='store_true', help='Update existing processing status records')
    parser.add_argument('--all', action='store_true', help='Create missing and update existing records')
    
    args = parser.parse_args()
    
    # Default to --all if no specific action is specified
    if not (args.create_missing or args.update_existing):
        args.all = True
    
    try:
        print("🔄 Starting Processing Status Update...")
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"⚠️  Service account file not found: {SERVICE_ACCOUNT_FILE}")
            print("Please ensure the service account key file is in the correct location.")
            return 1
        
        # Start Cloud SQL Proxy
        print("🚀 Starting Cloud SQL Proxy...")
        proxy_process, port = start_cloud_sql_proxy()
        
        # Create connection
        engine = create_connection(port)
        
        # Create missing records if requested
        if args.create_missing or args.all:
            print("\n📝 Creating missing processing status records...")
            create_missing_processing_status_records(engine)
        
        # Update existing records if requested
        if args.update_existing or args.all:
            print("\n🔄 Updating processing status records...")
            updated_count = update_all_processing_status(engine, args.limit)
            
            if updated_count > 0:
                # Show summary
                get_update_summary(engine)
        
        print(f"\n✅ Processing status update completed!")
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
    sys.exit(main())
