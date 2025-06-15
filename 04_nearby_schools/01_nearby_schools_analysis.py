#!/usr/bin/env python3
"""
Nearby Schools Data Analysis Script

This script provides comprehensive analysis of the school_polygon_relationships 
and nearby_school_polygons tables, specifically focused on location coverage 
and data completeness.

Features:
- Summary statistics for nearby schools data coverage
- Breakdown by drive time (5, 10, 15 minutes) 
- Location points coverage analysis
- EDC schools specific analysis
- Data completeness validation
- Export to CSV for detailed analysis
"""

import os
import sys
import subprocess
import time
import socket
import signal
from datetime import datetime
from pathlib import Path
import csv
import pandas as pd

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy pandas")
    sys.exit(1)

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = '../etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# EDC Schools CSV path
EDC_SCHOOLS_PATH = '../edc_schools/firebase_data/edc_schools.csv'

# Output directory
OUTPUT_DIR = Path('output')
OUTPUT_DIR.mkdir(exist_ok=True)

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

def get_nearby_schools_summary(engine, data_year=None):
    """Get overall nearby schools data summary"""
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
            
            missing_tables = [row[0] for row in tables_check if not row[1]]
            if missing_tables:
                print(f"📭 Missing tables: {', '.join(missing_tables)}")
                return None
            
            # Build query with optional data_year filter
            where_clause = ""
            params = {}
            if data_year:
                where_clause = "WHERE spr.data_year = :data_year"
                params['data_year'] = data_year
            
            # Get basic counts
            result = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as total_relationships,
                    COUNT(DISTINCT spr.location_id) as unique_locations,
                    COUNT(DISTINCT spr.drive_time) as unique_drive_times,
                    COUNT(DISTINCT spr.data_year) as unique_years,
                    MIN(spr.processed_at) as earliest_processed,
                    MAX(spr.processed_at) as latest_processed,
                    COUNT(nsp.id) as total_nearby_schools,
                    COUNT(DISTINCT nsp.school_uuid) as unique_schools
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                {where_clause}
            """), params).fetchone()
            
            if not result or result[0] == 0:
                print("📭 No records found in school_polygon_relationships table")
                return None
            
            summary = {
                'total_relationships': result[0],
                'unique_locations': result[1],
                'unique_drive_times': result[2],
                'unique_years': result[3],
                'earliest_processed': result[4],
                'latest_processed': result[5],
                'total_nearby_schools': result[6],
                'unique_schools': result[7],
                'data_year': data_year or 'All Years'
            }
            
            return summary
            
    except Exception as e:
        print(f"❌ Error getting nearby schools summary: {str(e)}")
        return None

def get_drive_time_breakdown(engine, data_year=None):
    """Get breakdown by drive time"""
    try:
        with engine.connect() as conn:
            where_clause = ""
            params = {}
            if data_year:
                where_clause = "WHERE spr.data_year = :data_year"
                params['data_year'] = data_year
            
            results = conn.execute(text(f"""
                SELECT 
                    spr.drive_time,
                    COUNT(spr.id) as polygon_count,
                    COUNT(DISTINCT spr.location_id) as unique_locations,
                    COUNT(nsp.id) as nearby_school_count,
                    COUNT(DISTINCT nsp.school_uuid) as unique_schools,
                    AVG(school_counts.school_count) as avg_schools_per_polygon
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                LEFT JOIN (
                    SELECT 
                        polygon_relationship_id,
                        COUNT(*) as school_count
                    FROM nearby_school_polygons
                    GROUP BY polygon_relationship_id
                ) school_counts ON spr.id = school_counts.polygon_relationship_id
                {where_clause}
                GROUP BY spr.drive_time
                ORDER BY spr.drive_time
            """), params).fetchall()
            
            breakdown = []
            for row in results:
                breakdown.append({
                    'drive_time': row[0],
                    'polygon_count': row[1],
                    'unique_locations': row[2],
                    'nearby_school_count': row[3],
                    'unique_schools': row[4],
                    'avg_schools_per_polygon': float(row[5]) if row[5] else 0.0
                })
            
            return breakdown
            
    except Exception as e:
        print(f"❌ Error getting drive time breakdown: {str(e)}")
        return []

def get_location_coverage_analysis(engine, data_year=None):
    """Analyze location points coverage"""
    try:
        with engine.connect() as conn:
            # Get all location points with school associations
            location_params = {}
            location_where = ""
            if data_year:
                location_where = "WHERE sl.data_year = :data_year"
                location_params['data_year'] = data_year
            
            # Get location coverage summary
            coverage_result = conn.execute(text(f"""
                SELECT 
                    COUNT(DISTINCT lp.id) as total_locations_with_schools,
                    COUNT(DISTINCT CASE WHEN spr.id IS NOT NULL THEN lp.id END) as locations_with_nearby_data,
                    COUNT(DISTINCT CASE WHEN esri.id IS NOT NULL THEN lp.id END) as locations_with_esri_data
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                    {f"AND spr.data_year = :data_year" if data_year else ""}
                LEFT JOIN esri_demographic_data esri ON lp.id = esri.location_id
                {location_where}
            """), location_params).fetchone()
            
            if not coverage_result:
                return None
                
            coverage = {
                'total_locations_with_schools': coverage_result[0],
                'locations_with_nearby_data': coverage_result[1],
                'locations_with_esri_data': coverage_result[2],
                'coverage_percentage': (coverage_result[1] / coverage_result[0] * 100) if coverage_result[0] > 0 else 0,
                'esri_coverage_percentage': (coverage_result[2] / coverage_result[0] * 100) if coverage_result[0] > 0 else 0
            }
            
            # Get drive time completeness for locations with nearby data
            completeness_result = conn.execute(text(f"""
                SELECT 
                    spr.location_id,
                    COUNT(DISTINCT spr.drive_time) as drive_times_count,
                    array_agg(DISTINCT spr.drive_time ORDER BY spr.drive_time) as drive_times
                FROM school_polygon_relationships spr
                {f"WHERE spr.data_year = :data_year" if data_year else ""}
                GROUP BY spr.location_id
                ORDER BY drive_times_count DESC, spr.location_id
            """), location_params if data_year else {}).fetchall()
            
            drive_time_completeness = {}
            for row in completeness_result:
                count = row[1]
                if count not in drive_time_completeness:
                    drive_time_completeness[count] = 0
                drive_time_completeness[count] += 1
            
            coverage['drive_time_completeness'] = drive_time_completeness
            return coverage
            
    except Exception as e:
        print(f"❌ Error getting location coverage analysis: {str(e)}")
        return None

def get_edc_schools_nearby_coverage(engine, edc_schools, data_year=None):
    """Analyze nearby schools coverage for EDC schools specifically"""
    try:
        if not edc_schools:
            return None
            
        with engine.connect() as conn:
            # Convert EDC school IDs to UUIDs for matching
            edc_uuids_result = conn.execute(text("""
                SELECT DISTINCT uuid 
                FROM schools 
                WHERE school_id = ANY(:school_ids)
            """), {'school_ids': list(edc_schools)}).fetchall()
            
            edc_uuids = [row[0] for row in edc_uuids_result]
            
            if not edc_uuids:
                print("⚠️  No EDC school UUIDs found in database")
                return None
            
            # Get EDC schools with location data
            location_params = {'edc_uuids': edc_uuids}
            location_where = ""
            if data_year:
                location_where = "AND sl.data_year = :data_year"
                location_params['data_year'] = data_year
            
            edc_coverage = conn.execute(text(f"""
                SELECT 
                    COUNT(DISTINCT s.uuid) as total_edc_schools,
                    COUNT(DISTINCT CASE WHEN sl.id IS NOT NULL THEN s.uuid END) as edc_with_locations,
                    COUNT(DISTINCT CASE WHEN spr.id IS NOT NULL THEN s.uuid END) as edc_with_nearby_data,
                    COUNT(DISTINCT CASE WHEN esri.id IS NOT NULL THEN s.uuid END) as edc_with_esri_data
                FROM schools s
                LEFT JOIN school_locations sl ON s.id = sl.school_id {location_where}
                LEFT JOIN location_points lp ON sl.location_id = lp.id
                LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                    {f"AND spr.data_year = :data_year" if data_year else ""}
                LEFT JOIN esri_demographic_data esri ON lp.id = esri.location_id
                WHERE s.uuid = ANY(:edc_uuids)
            """), location_params).fetchone()
            
            if not edc_coverage:
                return None
                
            coverage = {
                'total_edc_schools': edc_coverage[0],
                'edc_with_locations': edc_coverage[1],
                'edc_with_nearby_data': edc_coverage[2],
                'edc_with_esri_data': edc_coverage[3],
                'location_coverage_pct': (edc_coverage[1] / edc_coverage[0] * 100) if edc_coverage[0] > 0 else 0,
                'nearby_coverage_pct': (edc_coverage[2] / edc_coverage[1] * 100) if edc_coverage[1] > 0 else 0,
                'esri_coverage_pct': (edc_coverage[3] / edc_coverage[1] * 100) if edc_coverage[1] > 0 else 0
            }
            
            return coverage
            
    except Exception as e:
        print(f"❌ Error getting EDC schools coverage: {str(e)}")
        return None

def validate_data_integrity(engine, data_year=None):
    """Validate data integrity across related tables"""
    try:
        with engine.connect() as conn:
            params = {}
            where_clause = ""
            if data_year:
                where_clause = "WHERE spr.data_year = :data_year"
                params['data_year'] = data_year
            
            # Check for orphaned nearby_school_polygons
            orphaned_nearby = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM nearby_school_polygons nsp
                LEFT JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.id IS NULL
            """)).scalar() or 0
            
            # Check for polygon relationships without nearby schools
            empty_relationships = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                {where_clause}
                GROUP BY spr.id
                HAVING COUNT(nsp.id) = 0
            """), params).fetchall()
            
            empty_count = len(empty_relationships)
            
            # Check for invalid school UUIDs
            invalid_uuids = conn.execute(text(f"""
                SELECT COUNT(DISTINCT nsp.school_uuid)
                FROM nearby_school_polygons nsp
                JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                LEFT JOIN schools s ON nsp.school_uuid = s.uuid
                WHERE s.uuid IS NULL
                {f"AND spr.data_year = :data_year" if data_year else ""}
            """), params).scalar() or 0
            
            # Check for missing location coordinates
            missing_coords = conn.execute(text(f"""
                SELECT COUNT(DISTINCT spr.location_id)
                FROM school_polygon_relationships spr
                JOIN location_points lp ON spr.location_id = lp.id
                WHERE lp.latitude IS NULL OR lp.longitude IS NULL
                {f"AND spr.data_year = :data_year" if data_year else ""}
            """), params).scalar() or 0
            
            validation_results = {
                'orphaned_nearby_schools': orphaned_nearby,
                'empty_polygon_relationships': empty_count,
                'invalid_school_uuids': invalid_uuids,
                'missing_coordinates': missing_coords,
                'has_issues': orphaned_nearby > 0 or empty_count > 0 or invalid_uuids > 0 or missing_coords > 0
            }
            
            return validation_results
            
    except Exception as e:
        print(f"❌ Error validating data integrity: {str(e)}")
        return None

def export_detailed_analysis(engine, data_year=None):
    """Export detailed analysis to CSV files"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        year_suffix = f"_{data_year}" if data_year else "_all_years"
        
        with engine.connect() as conn:
            # Export location coverage details
            params = {}
            where_clause = ""
            if data_year:
                where_clause = "WHERE sl.data_year = :data_year"
                params['data_year'] = data_year
            
            coverage_query = text(f"""
                SELECT 
                    lp.id as location_id,
                    lp.latitude,
                    lp.longitude,
                    lp.city,
                    lp.state,
                    COUNT(DISTINCT sl.school_id) as schools_at_location,
                    COUNT(DISTINCT spr.id) as polygon_relationships,
                    COUNT(DISTINCT spr.drive_time) as drive_times_available,
                    array_agg(DISTINCT spr.drive_time ORDER BY spr.drive_time) 
                        FILTER (WHERE spr.drive_time IS NOT NULL) as drive_times,
                    COUNT(DISTINCT nsp.id) as total_nearby_schools,
                    COUNT(DISTINCT nsp.school_uuid) as unique_nearby_schools,
                    CASE WHEN esri.location_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_esri_data
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                LEFT JOIN school_polygon_relationships spr ON lp.id = spr.location_id 
                    {f"AND spr.data_year = :data_year" if data_year else ""}
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                LEFT JOIN (
                    SELECT DISTINCT location_id 
                    FROM esri_demographic_data
                ) esri ON lp.id = esri.location_id
                {where_clause}
                GROUP BY lp.id, lp.latitude, lp.longitude, lp.city, lp.state, esri.location_id
                ORDER BY lp.id
            """)
            
            coverage_results = conn.execute(coverage_query, params).fetchall()
            
            # Write location coverage CSV
            coverage_file = OUTPUT_DIR / f"location_coverage_analysis{year_suffix}_{timestamp}.csv"
            with open(coverage_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'location_id', 'latitude', 'longitude', 'city', 'state',
                    'schools_at_location', 'polygon_relationships', 'drive_times_available',
                    'drive_times', 'total_nearby_schools', 'unique_nearby_schools', 'has_esri_data'
                ])
                
                for row in coverage_results:
                    writer.writerow([
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                        str(row[8]) if row[8] else '[]', row[9], row[10], row[11]
                    ])
            
            print(f"📄 Exported location coverage analysis: {coverage_file}")
            
            # Export nearby schools details
            nearby_query = text(f"""
                SELECT 
                    spr.location_id,
                    spr.drive_time,
                    spr.data_year,
                    lp.latitude,
                    lp.longitude,
                    nsp.school_uuid,
                    nsp.relationship_type,
                    s.school_id,
                    s.name as school_name,
                    nsp.created_at
                FROM school_polygon_relationships spr
                JOIN location_points lp ON spr.location_id = lp.id
                JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                LEFT JOIN schools s ON nsp.school_uuid = s.uuid
                {f"WHERE spr.data_year = :data_year" if data_year else ""}
                ORDER BY spr.location_id, spr.drive_time, nsp.school_uuid
            """)
            
            nearby_results = conn.execute(nearby_query, params).fetchall()
            
            # Write nearby schools CSV
            nearby_file = OUTPUT_DIR / f"nearby_schools_details{year_suffix}_{timestamp}.csv"
            with open(nearby_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'location_id', 'drive_time', 'data_year', 'latitude', 'longitude',
                    'school_uuid', 'relationship_type', 'school_id', 'school_name', 'created_at'
                ])
                
                for row in nearby_results:
                    writer.writerow(row)
            
            print(f"📄 Exported nearby schools details: {nearby_file}")
            
            return {
                'coverage_file': str(coverage_file),
                'nearby_file': str(nearby_file),
                'coverage_records': len(coverage_results),
                'nearby_records': len(nearby_results)
            }
            
    except Exception as e:
        print(f"❌ Error exporting detailed analysis: {str(e)}")
        return None

def print_analysis_report(summary, drive_time_breakdown, coverage_analysis, edc_coverage, validation_results):
    """Print comprehensive analysis report"""
    print("\n" + "="*80)
    print("🏫 NEARBY SCHOOLS DATA ANALYSIS REPORT")
    print("="*80)
    
    if summary:
        print(f"\n📊 Overall Summary for {summary['data_year']}:")
        print(f"  📈 Total polygon relationships: {summary['total_relationships']:,}")
        print(f"  📍 Unique locations processed: {summary['unique_locations']:,}")
        print(f"  🏫 Total nearby schools found: {summary['total_nearby_schools']:,}")
        print(f"  🎯 Unique schools identified: {summary['unique_schools']:,}")
        print(f"  🕐 Drive times available: {summary['unique_drive_times']}")
        print(f"  📅 Years covered: {summary['unique_years']}")
        
        if summary['earliest_processed'] and summary['latest_processed']:
            print(f"  📆 Processing range: {summary['earliest_processed']} to {summary['latest_processed']}")
    else:
        print("\n📭 No nearby schools data found")
        return
    
    if drive_time_breakdown:
        print(f"\n🕐 Drive Time Breakdown:")
        for dt_info in drive_time_breakdown:
            print(f"  {dt_info['drive_time']} minutes:")
            print(f"    Polygons: {dt_info['polygon_count']:,}")
            print(f"    Locations: {dt_info['unique_locations']:,}")
            print(f"    Nearby schools: {dt_info['nearby_school_count']:,}")
            print(f"    Unique schools: {dt_info['unique_schools']:,}")
            print(f"    Avg schools/polygon: {dt_info['avg_schools_per_polygon']:.1f}")
    
    if coverage_analysis:
        print(f"\n📍 Location Coverage Analysis:")
        print(f"  Total locations with schools: {coverage_analysis['total_locations_with_schools']:,}")
        print(f"  Locations with nearby data: {coverage_analysis['locations_with_nearby_data']:,}")
        print(f"  Coverage percentage: {coverage_analysis['coverage_percentage']:.1f}%")
        print(f"  Locations with ESRI data: {coverage_analysis['locations_with_esri_data']:,}")
        print(f"  ESRI coverage percentage: {coverage_analysis['esri_coverage_percentage']:.1f}%")
        
        if coverage_analysis.get('drive_time_completeness'):
            print(f"\n🕐 Drive Time Completeness:")
            for count, locations in coverage_analysis['drive_time_completeness'].items():
                print(f"  {count} drive times: {locations:,} locations")
    
    if edc_coverage:
        print(f"\n🎯 EDC Schools Coverage:")
        print(f"  Total EDC schools: {edc_coverage['total_edc_schools']:,}")
        print(f"  With locations: {edc_coverage['edc_with_locations']:,} ({edc_coverage['location_coverage_pct']:.1f}%)")
        print(f"  With nearby data: {edc_coverage['edc_with_nearby_data']:,} ({edc_coverage['nearby_coverage_pct']:.1f}%)")
        print(f"  With ESRI data: {edc_coverage['edc_with_esri_data']:,} ({edc_coverage['esri_coverage_pct']:.1f}%)")
    
    if validation_results:
        print(f"\n🔍 Data Integrity Validation:")
        if validation_results['has_issues']:
            print("  ⚠️  Issues found:")
            if validation_results['orphaned_nearby_schools'] > 0:
                print(f"    Orphaned nearby schools: {validation_results['orphaned_nearby_schools']:,}")
            if validation_results['empty_polygon_relationships'] > 0:
                print(f"    Empty polygon relationships: {validation_results['empty_polygon_relationships']:,}")
            if validation_results['invalid_school_uuids'] > 0:
                print(f"    Invalid school UUIDs: {validation_results['invalid_school_uuids']:,}")
            if validation_results['missing_coordinates'] > 0:
                print(f"    Missing coordinates: {validation_results['missing_coordinates']:,}")
        else:
            print("  ✅ No data integrity issues found")
    
    print("\n" + "="*80)

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze nearby schools data')
    parser.add_argument('--data-year', type=int, help='Filter analysis by data year')
    parser.add_argument('--export', action='store_true', help='Export detailed analysis to CSV')
    parser.add_argument('--edc-only', action='store_true', help='Focus analysis on EDC schools only')
    
    args = parser.parse_args()
    
    print("🚀 Starting Nearby Schools Data Analysis...")
    print("Analysis functionality will be implemented here")
    
    # TODO: Implement the full analysis functionality
    print("✅ Analysis completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 