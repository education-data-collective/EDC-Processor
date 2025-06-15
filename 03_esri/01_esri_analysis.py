#!/usr/bin/env python3
"""
ESRI Demographic Data Analysis Script

This script provides comprehensive analysis of the esri_demographic_data table,
specifically focused on EDC schools from the Firebase schools CSV.

Features:
- Summary statistics for ESRI demographic data coverage
- Breakdown by drive time (5, 10, 15 minutes)
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

def get_esri_data_summary(engine):
    """Get overall ESRI demographic data summary"""
    try:
        with engine.connect() as conn:
            # Check if table exists
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'esri_demographic_data'
                );
            """)).scalar()
            
            if not table_exists:
                print("📭 No esri_demographic_data table found")
                return None
            
            # Get basic counts
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT location_id) as unique_locations,
                    COUNT(DISTINCT drive_time) as unique_drive_times,
                    MIN(timestamp) as earliest_processed,
                    MAX(timestamp) as latest_processed,
                    COUNT(CASE WHEN has_data = 1 THEN 1 END) as records_with_data,
                    COUNT(CASE WHEN timestamp >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as recent_records
                FROM esri_demographic_data
            """)).fetchone()
            
            if not result or result[0] == 0:
                print("📭 No records found in esri_demographic_data table")
                return None
            
            summary = {
                'total_records': result[0],
                'unique_locations': result[1],
                'unique_drive_times': result[2],
                'earliest_processed': result[3],
                'latest_processed': result[4],
                'records_with_data': result[5],
                'recent_records': result[6]
            }
            
            return summary
            
    except Exception as e:
        print(f"❌ Error getting ESRI data summary: {str(e)}")
        return None

def get_drive_time_breakdown(engine):
    """Get breakdown by drive time"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    drive_time,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT location_id) as unique_locations,
                    COUNT(CASE WHEN has_data = 1 THEN 1 END) as records_with_data,
                    CAST(ROUND(CAST(AVG(CASE WHEN medhinc_cy IS NOT NULL THEN medhinc_cy END) AS numeric), 0) AS integer) as avg_median_income,
                    ROUND(CAST(AVG(CASE WHEN per_hisp_child_20 IS NOT NULL THEN per_hisp_child_20 END) AS numeric), 3) as avg_hispanic_child_pct,
                    ROUND(CAST(AVG(CASE WHEN per_wht_child_20 IS NOT NULL THEN per_wht_child_20 END) AS numeric), 3) as avg_white_child_pct
                FROM esri_demographic_data
                GROUP BY drive_time
                ORDER BY drive_time
            """)).fetchall()
            
            return result
            
    except Exception as e:
        print(f"❌ Error getting drive time breakdown: {str(e)}")
        return []

def get_data_completeness_analysis(engine):
    """Analyze data completeness across different demographic fields"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(age4_cy) as age_data_count,
                    COUNT(medhinc_cy) as income_data_count,
                    COUNT(per_hisp_child_20) as hispanic_child_data_count,
                    COUNT(per_wht_child_20) as white_child_data_count,
                    COUNT(per_blk_child_20) as black_child_data_count,
                    COUNT(per_asn_child_20) as asian_child_data_count,
                    COUNT(per_hisp_adult_20) as hispanic_adult_data_count,
                    COUNT(per_wht_adult_20) as white_adult_data_count,
                    COUNT(per_blk_adult_20) as black_adult_data_count,
                    COUNT(per_asn_adult_20) as asian_adult_data_count,
                    COUNT(per_50k_cy) as income_bracket_data_count,
                    COUNT(per_renter_cy) as renter_data_count,
                    COUNT(per_vacant_cy) as vacancy_data_count,
                    COUNT(drive_time_polygon) as polygon_data_count
                FROM esri_demographic_data
            """)).fetchone()
            
            return result
            
    except Exception as e:
        print(f"❌ Error getting data completeness analysis: {str(e)}")
        return None

def get_edc_schools_esri_coverage(engine, edc_schools):
    """Analyze ESRI coverage specifically for EDC schools"""
    try:
        if not edc_schools:
            print("⚠️  No EDC schools loaded")
            return None
        
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
        
        print(f"📊 Found {len(hyphenated_schools)} hyphenated schools and {len(non_hyphenated_schools)} non-hyphenated schools")
        
        with engine.connect() as conn:
            all_results = []
            
            # UPDATED APPROACH: Check both direct school-location links AND mapping table
            
            # Handle hyphenated schools (match ncessch + split_suffix)
            if hyphenated_schools:
                hyphenated_conditions = []
                params = {}
                
                for i, (nces_id, suffix, original_id) in enumerate(hyphenated_schools):
                    hyphenated_conditions.append(f"(sd.ncessch = :nces_{i} AND sd.split_suffix = :suffix_{i})")
                    params[f'nces_{i}'] = nces_id
                    params[f'suffix_{i}'] = suffix
                
                # First try direct school-location approach
                hyphenated_query = f"""
                    SELECT 
                        CONCAT(sd.ncessch, '-', sd.split_suffix) as original_school_id,
                        sd.ncessch,
                        sd.split_suffix,
                        sl.location_id,
                        lp.latitude,
                        lp.longitude,
                        COUNT(ed.id) as esri_record_count,
                        COUNT(CASE WHEN ed.has_data = 1 THEN 1 END) as esri_records_with_data,
                        COUNT(DISTINCT ed.drive_time) as unique_drive_times,
                        MAX(ed.timestamp) as latest_esri_processing,
                        'hyphenated_direct' as match_type
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN esri_demographic_data ed ON ed.location_id = lp.id
                    WHERE ({' OR '.join(hyphenated_conditions)})
                    AND sd.is_current = true
                    AND sl.is_current = true
                    GROUP BY sd.ncessch, sd.split_suffix, sl.location_id, lp.latitude, lp.longitude
                """
                
                hyphenated_results = conn.execute(text(hyphenated_query), params).fetchall()
                all_results.extend(hyphenated_results)
                print(f"✅ Found {len(hyphenated_results)} direct matches for hyphenated schools")
                
                # Now check mapping table for any hyphenated schools not found above
                found_hyphenated_ids = set([result[0] for result in hyphenated_results])
                missing_hyphenated = [school_id for (_, _, school_id) in hyphenated_schools if school_id not in found_hyphenated_ids]
                
                if missing_hyphenated:
                    print(f"🔍 Checking mapping table for {len(missing_hyphenated)} missing hyphenated schools")
                    hyphenated_mapping_conditions = []
                    mapping_params = {}
                    
                    for i, school_id in enumerate(missing_hyphenated):
                        hyphenated_mapping_conditions.append(f"tm.ncessch = :mapping_school_{i}")
                        mapping_params[f'mapping_school_{i}'] = school_id
                    
                    hyphenated_mapping_query = f"""
                        SELECT 
                            tm.ncessch as original_school_id,
                            SPLIT_PART(tm.ncessch, '-', 1) as ncessch,
                            SPLIT_PART(tm.ncessch, '-', 2) as split_suffix,
                            tm.location_id,
                            tm.esri_latitude as latitude,
                            tm.esri_longitude as longitude,
                            COUNT(ed.id) as esri_record_count,
                            COUNT(CASE WHEN ed.has_data = 1 THEN 1 END) as esri_records_with_data,
                            COUNT(DISTINCT ed.drive_time) as unique_drive_times,
                            MAX(ed.timestamp) as latest_esri_processing,
                            'hyphenated_mapping' as match_type
                        FROM temp_esri_migration_location_mapping tm
                        LEFT JOIN esri_demographic_data ed ON ed.location_id = tm.location_id
                        WHERE ({' OR '.join(hyphenated_mapping_conditions)})
                        GROUP BY tm.ncessch, tm.location_id, tm.esri_latitude, tm.esri_longitude
                    """
                    
                    mapping_results = conn.execute(text(hyphenated_mapping_query), mapping_params).fetchall()
                    all_results.extend(mapping_results)
                    print(f"✅ Found {len(mapping_results)} additional mapping matches for hyphenated schools")
            
            # Handle non-hyphenated schools (direct ncessch match)
            if non_hyphenated_schools:
                non_hyphenated_str = ','.join([f"'{school_id}'" for school_id in non_hyphenated_schools])
                
                # First try direct school-location approach
                non_hyphenated_query = f"""
                    SELECT 
                        sd.ncessch as original_school_id,
                        sd.ncessch,
                        sd.split_suffix,
                        sl.location_id,
                        lp.latitude,
                        lp.longitude,
                        COUNT(ed.id) as esri_record_count,
                        COUNT(CASE WHEN ed.has_data = 1 THEN 1 END) as esri_records_with_data,
                        COUNT(DISTINCT ed.drive_time) as unique_drive_times,
                        MAX(ed.timestamp) as latest_esri_processing,
                        'direct' as match_type
                    FROM school_directory sd
                    JOIN schools s ON sd.school_id = s.id
                    JOIN school_locations sl ON s.id = sl.school_id
                    JOIN location_points lp ON sl.location_id = lp.id
                    LEFT JOIN esri_demographic_data ed ON ed.location_id = lp.id
                    WHERE sd.ncessch IN ({non_hyphenated_str})
                    AND sd.is_current = true
                    AND sl.is_current = true
                    GROUP BY sd.ncessch, sd.split_suffix, sl.location_id, lp.latitude, lp.longitude
                """
                
                non_hyphenated_results = conn.execute(text(non_hyphenated_query)).fetchall()
                all_results.extend(non_hyphenated_results)
                print(f"✅ Found {len(non_hyphenated_results)} direct matches for non-hyphenated schools")
                
                # Now check mapping table for any non-hyphenated schools not found above  
                found_non_hyphenated_ids = set([result[0] for result in non_hyphenated_results])
                missing_non_hyphenated = [school_id for school_id in non_hyphenated_schools if school_id not in found_non_hyphenated_ids]
                
                if missing_non_hyphenated:
                    print(f"🔍 Checking mapping table for {len(missing_non_hyphenated)} missing non-hyphenated schools")
                    missing_non_hyphenated_str = ','.join([f"'{school_id}'" for school_id in missing_non_hyphenated])
                    
                    non_hyphenated_mapping_query = f"""
                        SELECT 
                            tm.ncessch as original_school_id,
                            tm.ncessch,
                            NULL as split_suffix,
                            tm.location_id,
                            tm.esri_latitude as latitude,
                            tm.esri_longitude as longitude,
                            COUNT(ed.id) as esri_record_count,
                            COUNT(CASE WHEN ed.has_data = 1 THEN 1 END) as esri_records_with_data,
                            COUNT(DISTINCT ed.drive_time) as unique_drive_times,
                            MAX(ed.timestamp) as latest_esri_processing,
                            'direct_mapping' as match_type
                        FROM temp_esri_migration_location_mapping tm
                        LEFT JOIN esri_demographic_data ed ON ed.location_id = tm.location_id
                        WHERE tm.ncessch IN ({missing_non_hyphenated_str})
                        GROUP BY tm.ncessch, tm.location_id, tm.esri_latitude, tm.esri_longitude
                    """
                    
                    mapping_results = conn.execute(text(non_hyphenated_mapping_query)).fetchall()
                    all_results.extend(mapping_results)
                    print(f"✅ Found {len(mapping_results)} additional mapping matches for non-hyphenated schools")
            
            # DEDUPLICATION: Remove duplicate schools (same school_id found via multiple pathways)
            # Keep the best match for each school (prioritize direct matches over mapping matches)
            school_results = {}
            match_priority = {
                'hyphenated_direct': 1,
                'direct': 2, 
                'hyphenated_mapping': 3,
                'direct_mapping': 4
            }
            
            for row in all_results:
                school_id = row[0]  # original_school_id
                match_type = row[10]  # match_type
                
                if school_id not in school_results:
                    school_results[school_id] = row
                else:
                    # Keep the result with higher priority (lower number = higher priority)
                    current_priority = match_priority.get(school_results[school_id][10], 999)
                    new_priority = match_priority.get(match_type, 999)
                    
                    if new_priority < current_priority:
                        school_results[school_id] = row
            
            # Convert back to list for compatibility
            deduplicated_results = list(school_results.values())
            
            print(f"🔍 Deduplication: {len(all_results)} total matches → {len(deduplicated_results)} unique schools")
            
            # Summary statistics (using deduplicated results)
            total_edc_schools_with_locations = len(deduplicated_results)
            edc_schools_with_esri = len([row for row in deduplicated_results if row[6] > 0])  # esri_record_count > 0
            edc_schools_with_complete_esri = len([row for row in deduplicated_results if row[8] == 3])  # unique_drive_times == 3
            
            # Count by match type for debugging (using deduplicated results)
            match_type_counts = {}
            for row in deduplicated_results:
                match_type = row[10]  # match_type is at index 10
                match_type_counts[match_type] = match_type_counts.get(match_type, 0) + 1
            
            print(f"📊 Match type breakdown (after deduplication): {match_type_counts}")
            
            # Calculate unique location statistics
            unique_locations = set([row[3] for row in deduplicated_results])  # location_id at index 3
            unique_coordinates = set([(row[4], row[5]) for row in deduplicated_results if row[4] and row[5]])  # lat, lng
            
            print(f"📍 Location analysis:")
            print(f"  Unique locations: {len(unique_locations)}")
            print(f"  Unique coordinate pairs: {len(unique_coordinates)}")
            print(f"  Schools sharing locations: {total_edc_schools_with_locations - len(unique_locations)}")
            
            return {
                'total_edc_schools': len(edc_schools),
                'total_edc_schools_with_locations': total_edc_schools_with_locations,
                'edc_schools_with_esri': edc_schools_with_esri,
                'edc_schools_with_complete_esri': edc_schools_with_complete_esri,
                'detailed_results': deduplicated_results,  # Use deduplicated results
                'unique_locations': len(unique_locations),
                'unique_coordinates': len(unique_coordinates),
                'hyphenated_matches': len(hyphenated_schools) if hyphenated_schools else 0,
                'non_hyphenated_matches': len(non_hyphenated_schools) if non_hyphenated_schools else 0,
                'match_type_counts': match_type_counts
            }
            
    except Exception as e:
        print(f"❌ Error getting EDC schools ESRI coverage: {str(e)}")
        return None

def export_detailed_analysis(engine, edc_schools):
    """Export detailed analysis to CSV files"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Get EDC schools coverage first to get the location IDs we care about
        edc_coverage = get_edc_schools_esri_coverage(engine, edc_schools)
        
        with engine.connect() as conn:
            # Export only ESRI data for EDC school locations
            if edc_coverage and edc_coverage['detailed_results']:
                # Get unique location IDs from EDC schools
                edc_location_ids = list(set([row[3] for row in edc_coverage['detailed_results']]))  # location_id is at index 3
                location_ids_str = ','.join([str(loc_id) for loc_id in edc_location_ids])
                
                edc_esri_data_result = conn.execute(text(f"""
                    SELECT 
                        ed.*,
                        sd.ncessch,
                        CASE WHEN sd.ncessch IS NOT NULL THEN 'Yes' ELSE 'No' END as has_school_connection
                    FROM esri_demographic_data ed
                    LEFT JOIN school_locations sl ON ed.location_id = sl.location_id
                    LEFT JOIN schools s ON sl.school_id = s.id
                    LEFT JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                    WHERE ed.location_id IN ({location_ids_str})
                    ORDER BY ed.timestamp DESC, ed.location_id, ed.drive_time
                """)).fetchall()
                
                if edc_esri_data_result:
                    edc_esri_data_file = OUTPUT_DIR / f'esri_edc_detailed_data_{timestamp}.csv'
                    columns = [
                        'id', 'location_id', 'drive_time',
                        'age4_cy', 'age5_cy', 'age6_cy', 'age7_cy', 'age8_cy', 'age9_cy',
                        'age10_cy', 'age11_cy', 'age12_cy', 'age13_cy', 'age14_cy', 
                        'age15_cy', 'age16_cy', 'age17_cy',
                        'age4_fy', 'age5_fy', 'age6_fy', 'age7_fy', 'age8_fy', 'age9_fy',
                        'age10_fy', 'age11_fy', 'age12_fy', 'age13_fy', 'age14_fy', 
                        'age15_fy', 'age16_fy', 'age17_fy',
                        'age4_c20', 'age5_c20', 'age6_c20', 'age7_c20', 'age8_c20', 'age9_c20',
                        'age10_c20', 'age11_c20', 'age12_c20', 'age13_c20', 'age14_c20', 
                        'age15_c20', 'age16_c20', 'age17_c20',
                        'per_hisp_adult_20', 'per_wht_adult_20', 'per_blk_adult_20', 'per_asn_adult_20',
                        'per_pi_adult_20', 'per_ai_adult_20', 'per_other_adult_20', 'per_two_or_more_adult_20',
                        'per_hisp_child_20', 'per_wht_child_20', 'per_blk_child_20', 'per_asn_child_20',
                        'per_pi_child_20', 'per_ai_child_20', 'per_other_child_20', 'per_two_or_more_child_20',
                        'medhinc_cy', 'per_50k_cy', 'per_renter_cy', 'per_vacant_cy',
                        'drive_time_polygon', 'timestamp', 'has_data',
                        'ncessch', 'has_school_connection'
                    ]
                    
                    with open(edc_esri_data_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)
                        writer.writerows(edc_esri_data_result)
                    
                    print(f"📄 Exported EDC ESRI detailed data to: {edc_esri_data_file} ({len(edc_esri_data_result)} rows)")
                else:
                    print("📭 No ESRI data found for EDC school locations")
            else:
                print("📭 No EDC school locations found for ESRI data export")
            
            # Export EDC schools specific analysis - reuse the coverage analysis results
            if edc_coverage and edc_coverage['detailed_results']:
                edc_file = OUTPUT_DIR / f'esri_edc_schools_analysis_{timestamp}.csv'
                
                # Convert results to export format
                export_data = []
                for row in edc_coverage['detailed_results']:
                    # Determine status
                    unique_drive_times = row[8]
                    total_records = row[6]
                    if unique_drive_times == 3:
                        status = 'Complete'
                    elif total_records > 0:
                        status = 'Partial'
                    else:
                        status = 'Missing'
                    
                    # Create drive times string
                    if total_records > 0:
                        # We'd need to query for actual drive times, for now use placeholder
                        drive_times = f"{unique_drive_times} drive times"
                    else:
                        drive_times = "None"
                    
                    export_data.append([
                        row[0],  # original_school_id
                        row[3],  # location_id
                        row[4],  # latitude
                        row[5],  # longitude
                        row[6],  # esri_record_count (total_esri_records)
                        row[7],  # esri_records_with_data
                        row[8],  # unique_drive_times
                        drive_times,  # drive_times_available
                        row[9],  # latest_esri_processing
                        status,  # esri_status
                        row[10]  # match_type
                    ])
                
                columns = [
                    'original_school_id', 'location_id', 'latitude', 'longitude',
                    'total_esri_records', 'esri_records_with_data', 'unique_drive_times',
                    'drive_times_available', 'latest_esri_processing', 'esri_status', 'match_type'
                ]
                
                with open(edc_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(export_data)
                
                print(f"📄 Exported EDC schools analysis to: {edc_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error exporting detailed analysis: {str(e)}")
        return False

def print_analysis_report(summary, drive_time_breakdown, completeness_analysis, edc_coverage):
    """Print comprehensive analysis report"""
    print("\n" + "="*80)
    print("📊 ESRI DEMOGRAPHIC DATA ANALYSIS REPORT")
    print("="*80)
    
    if summary:
        print(f"\n📈 OVERALL SUMMARY:")
        print(f"  Total records: {summary['total_records']:,}")
        print(f"  Unique locations: {summary['unique_locations']:,}")
        print(f"  Unique drive times: {summary['unique_drive_times']}")
        print(f"  Records with data: {summary['records_with_data']:,}")
        print(f"  Recent records (30 days): {summary['recent_records']:,}")
        print(f"  Data range: {summary['earliest_processed']} to {summary['latest_processed']}")
    
    if drive_time_breakdown:
        print(f"\n⏱️  DRIVE TIME BREAKDOWN:")
        for row in drive_time_breakdown:
            print(f"  {row[0]} minutes: {row[1]:,} records, {row[2]:,} locations, "
                  f"{row[3]:,} with data, avg income: ${row[4]:,.0f}" if row[4] else "N/A")
    
    if completeness_analysis:
        total = completeness_analysis[0]
        print(f"\n📋 DATA COMPLETENESS:")
        print(f"  Age data: {completeness_analysis[1]:,}/{total:,} ({completeness_analysis[1]/total*100:.1f}%)")
        print(f"  Income data: {completeness_analysis[2]:,}/{total:,} ({completeness_analysis[2]/total*100:.1f}%)")
        print(f"  Child demographic data: {completeness_analysis[3]:,}/{total:,} ({completeness_analysis[3]/total*100:.1f}%)")
        print(f"  Adult demographic data: {completeness_analysis[7]:,}/{total:,} ({completeness_analysis[7]/total*100:.1f}%)")
        print(f"  Polygon data: {completeness_analysis[14]:,}/{total:,} ({completeness_analysis[14]/total*100:.1f}%)")
    
    if edc_coverage:
        print(f"\n🏫 EDC SCHOOLS COVERAGE:")
        print(f"  Total EDC schools: {edc_coverage['total_edc_schools']:,}")
        print(f"  EDC schools with locations: {edc_coverage['total_edc_schools_with_locations']:,}")
        print(f"  EDC schools with ESRI data: {edc_coverage['edc_schools_with_esri']:,}")
        print(f"  EDC schools with complete ESRI: {edc_coverage['edc_schools_with_complete_esri']:,}")
        print(f"  Hyphenated school matches: {edc_coverage.get('hyphenated_matches', 0):,}")
        print(f"  Non-hyphenated school matches: {edc_coverage.get('non_hyphenated_matches', 0):,}")
        
        if edc_coverage['total_edc_schools_with_locations'] > 0:
            coverage_pct = (edc_coverage['edc_schools_with_esri'] / edc_coverage['total_edc_schools_with_locations']) * 100
            complete_pct = (edc_coverage['edc_schools_with_complete_esri'] / edc_coverage['total_edc_schools_with_locations']) * 100
            print(f"  Coverage rate: {coverage_pct:.1f}%")
            print(f"  Complete coverage rate: {complete_pct:.1f}%")
        
        # Show schools needing ESRI processing (updated indices for new result structure)
        missing_esri = [row for row in edc_coverage['detailed_results'] if row[6] == 0]  # esri_record_count == 0
        partial_esri = [row for row in edc_coverage['detailed_results'] if row[6] > 0 and row[8] < 3]  # unique_drive_times < 3
        
        # Get unique locations needing ESRI data
        missing_locations = set([row[3] for row in missing_esri])  # location_id is at index 3
        partial_locations = set([row[3] for row in partial_esri])
        
        print(f"\n📍 LOCATION-BASED ANALYSIS:")
        print(f"  Unique EDC locations: {edc_coverage.get('unique_locations', 'N/A'):,}")
        print(f"  Unique coordinate pairs: {edc_coverage.get('unique_coordinates', 'N/A'):,}")
        print(f"  Schools sharing locations: {edc_coverage['total_edc_schools_with_locations'] - edc_coverage.get('unique_locations', 0):,}")
        print(f"  Unique locations with ESRI data: {len(set([row[3] for row in edc_coverage['detailed_results'] if row[6] > 0])):,}")
        print(f"  Unique locations missing ESRI data: {len(missing_locations):,}")
        print(f"  Unique locations with partial ESRI data: {len(partial_locations):,}")
        
        if missing_esri:
            print(f"\n🚨 SCHOOLS MISSING ESRI DATA ({len(missing_esri)} schools at {len(missing_locations)} unique locations):")
            # Group by location to show which schools share locations
            location_to_schools = {}
            for row in missing_esri:
                location_id = row[3]
                school_id = row[0]
                if location_id not in location_to_schools:
                    location_to_schools[location_id] = []
                location_to_schools[location_id].append(school_id)
            
            count = 0
            for location_id, schools in location_to_schools.items():
                if count >= 10:  # Show first 10 locations
                    break
                if len(schools) == 1:
                    print(f"    Location {location_id}: {schools[0]}")
                else:
                    print(f"    Location {location_id}: {len(schools)} schools ({', '.join(schools[:3])}{'...' if len(schools) > 3 else ''})")
                count += 1
            
            if len(missing_locations) > 10:
                print(f"    ... and {len(missing_locations) - 10} more unique locations")
        
        if partial_esri:
            print(f"\n⚠️  SCHOOLS WITH PARTIAL ESRI DATA ({len(partial_esri)} schools at {len(partial_locations)} unique locations):")
            # Group by location to show which schools share locations
            location_to_schools = {}
            for row in partial_esri:
                location_id = row[3]
                school_id = row[0]
                drive_times = row[8]
                if location_id not in location_to_schools:
                    location_to_schools[location_id] = {'schools': [], 'drive_times': drive_times}
                location_to_schools[location_id]['schools'].append(school_id)
            
            count = 0
            for location_id, data in location_to_schools.items():
                if count >= 10:  # Show first 10 locations
                    break
                schools = data['schools']
                drive_times = data['drive_times']
                if len(schools) == 1:
                    print(f"    Location {location_id}: {schools[0]} ({drive_times}/3 drive times)")
                else:
                    print(f"    Location {location_id}: {len(schools)} schools ({drive_times}/3 drive times) - {', '.join(schools[:3])}{'...' if len(schools) > 3 else ''}")
                count += 1
            
            if len(partial_locations) > 10:
                print(f"    ... and {len(partial_locations) - 10} more unique locations")

def main():
    """Main execution function"""
    proxy_process = None
    
    try:
        print("🚀 Starting ESRI Demographic Data Analysis")
        print(f"📅 Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load EDC schools
        edc_schools = load_edc_schools()
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Run analyses
        print("\n📊 Running analyses...")
        summary = get_esri_data_summary(engine)
        drive_time_breakdown = get_drive_time_breakdown(engine)
        completeness_analysis = get_data_completeness_analysis(engine)
        edc_coverage = get_edc_schools_esri_coverage(engine, edc_schools)
        
        # Print report
        print_analysis_report(summary, drive_time_breakdown, completeness_analysis, edc_coverage)
        
        # Export detailed analysis
        print("\n📄 Exporting detailed analysis...")
        export_success = export_detailed_analysis(engine, edc_schools)
        
        if export_success:
            print(f"\n✅ Analysis completed successfully!")
            print(f"📁 Output files saved to: {OUTPUT_DIR.absolute()}")
        else:
            print(f"\n⚠️  Analysis completed with export errors")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    sys.exit(main()) 