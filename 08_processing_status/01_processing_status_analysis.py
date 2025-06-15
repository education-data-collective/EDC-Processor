#!/usr/bin/env python3
"""
Processing Status Analysis Script

This script provides comprehensive analysis of the processing_status table,
including both full database analysis and specific analysis for EDC schools.

Features:
- Summary statistics for all processing flags
- Breakdown by data_year and data_completeness
- EDC schools specific analysis
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

def get_processing_status_summary(engine):
    """Get overall processing status summary"""
    try:
        with engine.connect() as conn:
            # Get basic counts
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT school_id) as unique_schools,
                    COUNT(DISTINCT data_year) as unique_years,
                    MIN(data_year) as earliest_year,
                    MAX(data_year) as latest_year,
                    MIN(created_at) as earliest_created,
                    MAX(created_at) as latest_created,
                    MIN(last_processed_at) as earliest_processed,
                    MAX(last_processed_at) as latest_processed
                FROM processing_status
            """)).fetchone()
            
            if not result or result[0] == 0:
                print("📭 No records found in processing_status table")
                return None
            
            summary = {
                'total_records': result[0],
                'unique_schools': result[1],
                'unique_years': result[2],
                'earliest_year': result[3],
                'latest_year': result[4],
                'earliest_created': result[5],
                'latest_created': result[6],
                'earliest_processed': result[7],
                'latest_processed': result[8]
            }
            
            return summary
            
    except Exception as e:
        print(f"❌ Error getting processing status summary: {str(e)}")
        return None

def get_processing_flags_summary(engine):
    """Get summary of all processing flags"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    SUM(CASE WHEN enrollment_processed THEN 1 ELSE 0 END) as enrollment_processed_count,
                    SUM(CASE WHEN location_processed THEN 1 ELSE 0 END) as location_processed_count,
                    SUM(CASE WHEN characteristics_processed THEN 1 ELSE 0 END) as characteristics_processed_count,
                    SUM(CASE WHEN projections_processed THEN 1 ELSE 0 END) as projections_processed_count,
                    SUM(CASE WHEN demographics_processed THEN 1 ELSE 0 END) as demographics_processed_count,
                    SUM(CASE WHEN nces_processed THEN 1 ELSE 0 END) as nces_processed_count,
                    SUM(CASE WHEN geocoding_processed THEN 1 ELSE 0 END) as geocoding_processed_count,
                    SUM(CASE WHEN esri_processed THEN 1 ELSE 0 END) as esri_processed_count,
                    SUM(CASE WHEN district_metrics_processed THEN 1 ELSE 0 END) as district_metrics_processed_count,
                    COUNT(*) as total_records
                FROM processing_status
            """)).fetchone()
            
            if result:
                flags_summary = {
                    'enrollment_processed': {'count': result[0], 'percentage': (result[0] / result[9]) * 100 if result[9] > 0 else 0},
                    'location_processed': {'count': result[1], 'percentage': (result[1] / result[9]) * 100 if result[9] > 0 else 0},
                    'characteristics_processed': {'count': result[2], 'percentage': (result[2] / result[9]) * 100 if result[9] > 0 else 0},
                    'projections_processed': {'count': result[3], 'percentage': (result[3] / result[9]) * 100 if result[9] > 0 else 0},
                    'demographics_processed': {'count': result[4], 'percentage': (result[4] / result[9]) * 100 if result[9] > 0 else 0},
                    'nces_processed': {'count': result[5], 'percentage': (result[5] / result[9]) * 100 if result[9] > 0 else 0},
                    'geocoding_processed': {'count': result[6], 'percentage': (result[6] / result[9]) * 100 if result[9] > 0 else 0},
                    'esri_processed': {'count': result[7], 'percentage': (result[7] / result[9]) * 100 if result[9] > 0 else 0},
                    'district_metrics_processed': {'count': result[8], 'percentage': (result[8] / result[9]) * 100 if result[9] > 0 else 0},
                    'total_records': result[9]
                }
                return flags_summary
            
            return None
            
    except Exception as e:
        print(f"❌ Error getting processing flags summary: {str(e)}")
        return None

def get_data_completeness_summary(engine):
    """Get summary by data_completeness"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    data_completeness,
                    COUNT(*) as count,
                    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM processing_status) as percentage
                FROM processing_status
                GROUP BY data_completeness
                ORDER BY count DESC
            """)).fetchall()
            
            completeness_summary = {}
            for row in result:
                completeness_summary[row[0] or 'NULL'] = {
                    'count': row[1],
                    'percentage': float(row[2])
                }
            
            return completeness_summary
            
    except Exception as e:
        print(f"❌ Error getting data completeness summary: {str(e)}")
        return None

def get_data_year_summary(engine):
    """Get summary by data_year"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    data_year,
                    COUNT(*) as count,
                    COUNT(DISTINCT school_id) as unique_schools,
                    SUM(CASE WHEN enrollment_processed THEN 1 ELSE 0 END) as enrollment_processed,
                    SUM(CASE WHEN location_processed THEN 1 ELSE 0 END) as location_processed,
                    SUM(CASE WHEN demographics_processed THEN 1 ELSE 0 END) as demographics_processed,
                    SUM(CASE WHEN data_completeness = 'complete' THEN 1 ELSE 0 END) as complete_count
                FROM processing_status
                GROUP BY data_year
                ORDER BY data_year DESC
            """)).fetchall()
            
            year_summary = {}
            for row in result:
                year_summary[row[0]] = {
                    'total_records': row[1],
                    'unique_schools': row[2],
                    'enrollment_processed': row[3],
                    'location_processed': row[4],
                    'demographics_processed': row[5],
                    'complete_count': row[6]
                }
            
            return year_summary
            
    except Exception as e:
        print(f"❌ Error getting data year summary: {str(e)}")
        return None

def get_edc_schools_analysis(engine, edc_schools):
    """Analyze processing status for EDC schools specifically"""
    if not edc_schools:
        return None
    
    try:
        # Convert NCESSCH IDs to school IDs in database
        placeholders = ','.join([f"'{school_id}'" for school_id in edc_schools])
        
        with engine.connect() as conn:
            # First, find matching schools in our database
            school_mapping_query = text(f"""
                SELECT DISTINCT 
                    sd.id as school_id,
                    sd.ncessch,
                    sn.display_name,
                    sd.state_abbr,
                    sd.data_year
                FROM school_directory sd
                LEFT JOIN school_names sn ON sd.school_id = sn.school_id AND sn.is_active = true
                WHERE sd.ncessch IN ({placeholders})
                   OR SUBSTRING(sd.ncessch FROM 1 FOR LENGTH(sd.ncessch) - 3) IN ({placeholders})
                ORDER BY sd.ncessch, sd.data_year DESC
            """)
            
            school_results = conn.execute(school_mapping_query).fetchall()
            
            if not school_results:
                print(f"⚠️  No matching schools found in database for EDC schools")
                return None
            
            # Get school IDs for processing status query
            db_school_ids = [str(row[0]) for row in school_results]
            school_id_placeholders = ','.join(db_school_ids)
            
            # Get processing status for these schools
            ps_query = text(f"""
                SELECT 
                    COUNT(*) as total_edc_records,
                    COUNT(DISTINCT school_id) as unique_edc_schools,
                    SUM(CASE WHEN enrollment_processed THEN 1 ELSE 0 END) as enrollment_processed,
                    SUM(CASE WHEN location_processed THEN 1 ELSE 0 END) as location_processed,
                    SUM(CASE WHEN characteristics_processed THEN 1 ELSE 0 END) as characteristics_processed,
                    SUM(CASE WHEN projections_processed THEN 1 ELSE 0 END) as projections_processed,
                    SUM(CASE WHEN demographics_processed THEN 1 ELSE 0 END) as demographics_processed,
                    SUM(CASE WHEN nces_processed THEN 1 ELSE 0 END) as nces_processed,
                    SUM(CASE WHEN geocoding_processed THEN 1 ELSE 0 END) as geocoding_processed,
                    SUM(CASE WHEN esri_processed THEN 1 ELSE 0 END) as esri_processed,
                    SUM(CASE WHEN district_metrics_processed THEN 1 ELSE 0 END) as district_metrics_processed,
                    SUM(CASE WHEN data_completeness = 'complete' THEN 1 ELSE 0 END) as complete_count,
                    SUM(CASE WHEN data_completeness = 'partial' THEN 1 ELSE 0 END) as partial_count,
                    SUM(CASE WHEN data_completeness = 'none' THEN 1 ELSE 0 END) as none_count
                FROM processing_status
                WHERE school_id IN ({school_id_placeholders})
            """)
            
            ps_result = conn.execute(ps_query).fetchone()
            
            if ps_result and ps_result[0] > 0:
                edc_analysis = {
                    'total_edc_schools_in_csv': len(edc_schools),
                    'matched_schools_in_db': len(school_results),
                    'unique_schools_with_status': ps_result[1],
                    'total_status_records': ps_result[0],
                    'processing_flags': {
                        'enrollment_processed': {'count': ps_result[2], 'percentage': (ps_result[2] / ps_result[0]) * 100},
                        'location_processed': {'count': ps_result[3], 'percentage': (ps_result[3] / ps_result[0]) * 100},
                        'characteristics_processed': {'count': ps_result[4], 'percentage': (ps_result[4] / ps_result[0]) * 100},
                        'projections_processed': {'count': ps_result[5], 'percentage': (ps_result[5] / ps_result[0]) * 100},
                        'demographics_processed': {'count': ps_result[6], 'percentage': (ps_result[6] / ps_result[0]) * 100},
                        'nces_processed': {'count': ps_result[7], 'percentage': (ps_result[7] / ps_result[0]) * 100},
                        'geocoding_processed': {'count': ps_result[8], 'percentage': (ps_result[8] / ps_result[0]) * 100},
                        'esri_processed': {'count': ps_result[9], 'percentage': (ps_result[9] / ps_result[0]) * 100},
                        'district_metrics_processed': {'count': ps_result[10], 'percentage': (ps_result[10] / ps_result[0]) * 100},
                    },
                    'data_completeness': {
                        'complete': {'count': ps_result[11], 'percentage': (ps_result[11] / ps_result[0]) * 100},
                        'partial': {'count': ps_result[12], 'percentage': (ps_result[12] / ps_result[0]) * 100},
                        'none': {'count': ps_result[13], 'percentage': (ps_result[13] / ps_result[0]) * 100}
                    }
                }
                return edc_analysis
            else:
                print(f"⚠️  No processing status records found for EDC schools")
                return None
            
    except Exception as e:
        print(f"❌ Error analyzing EDC schools: {str(e)}")
        return None

def export_detailed_analysis(engine, edc_schools):
    """Export detailed analysis to CSV files"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        with engine.connect() as conn:
            # Export full processing status
            full_query = text("""
                SELECT 
                    ps.*,
                    sd.ncessch,
                    sn.display_name as school_name,
                    sd.state_abbr,
                    sd.system_name
                FROM processing_status ps
                LEFT JOIN schools s ON ps.school_id = s.id
                LEFT JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                LEFT JOIN school_names sn ON s.id = sn.school_id AND sn.is_active = true
                ORDER BY ps.school_id, ps.data_year
            """)
            
            full_results = conn.execute(full_query).fetchall()
            columns = [
                'id', 'school_id', 'data_year', 'enrollment_processed', 'location_processed',
                'characteristics_processed', 'projections_processed', 'demographics_processed',
                'nces_processed', 'geocoding_processed', 'esri_processed', 'district_metrics_processed',
                'data_completeness', 'last_processed_at', 'nces_processed_at', 'created_at', 'updated_at',
                'ncessch', 'school_name', 'state_abbr', 'system_name'
            ]
            
            full_df = pd.DataFrame(full_results, columns=columns)
            full_output_path = output_dir / f"processing_status_full_{timestamp}.csv"
            full_df.to_csv(full_output_path, index=False)
            print(f"📄 Full analysis exported to: {full_output_path}")
            
            # Export EDC schools specific analysis if available
            if edc_schools:
                placeholders = ','.join([f"'{school_id}'" for school_id in edc_schools])
                edc_query = text(f"""
                    SELECT 
                        ps.*,
                        sd.ncessch,
                        sn.display_name as school_name,
                        sd.state_abbr,
                        sd.system_name,
                        'YES' as is_edc_school
                    FROM processing_status ps
                    LEFT JOIN schools s ON ps.school_id = s.id
                    LEFT JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                    LEFT JOIN school_names sn ON s.id = sn.school_id AND sn.is_active = true
                    WHERE sd.ncessch IN ({placeholders})
                       OR SUBSTRING(sd.ncessch FROM 1 FOR LENGTH(sd.ncessch) - 3) IN ({placeholders})
                    ORDER BY ps.school_id, ps.data_year
                """)
                
                edc_results = conn.execute(edc_query).fetchall()
                edc_columns = columns + ['is_edc_school']
                
                edc_df = pd.DataFrame(edc_results, columns=edc_columns)
                edc_output_path = output_dir / f"processing_status_edc_schools_{timestamp}.csv"
                edc_df.to_csv(edc_output_path, index=False)
                print(f"📄 EDC schools analysis exported to: {edc_output_path}")
                
                return full_output_path, edc_output_path
            
            return full_output_path, None
            
    except Exception as e:
        print(f"❌ Error exporting detailed analysis: {str(e)}")
        return None, None

def print_analysis_report(summary, flags_summary, completeness_summary, year_summary, edc_analysis):
    """Print comprehensive analysis report"""
    print("\n" + "="*80)
    print("📊 PROCESSING STATUS ANALYSIS REPORT")
    print("="*80)
    
    # Check if we have any data at all
    if not summary or summary['total_records'] == 0:
        print(f"\n⚠️  PROCESSING STATUS TABLE IS EMPTY")
        print(f"  • No processing status records found in the database")
        print(f"  • Run the update script (02_processing_status_update.py) to create initial records")
        print(f"  • The update script will analyze existing data and populate the processing_status table")
        return
    
    # Overall summary
    if summary:
        print(f"\n📈 OVERALL SUMMARY:")
        print(f"  • Total records: {summary['total_records']:,}")
        print(f"  • Unique schools: {summary['unique_schools']:,}")
        print(f"  • Data years: {summary['unique_years']} (from {summary['earliest_year']} to {summary['latest_year']})")
        print(f"  • Record dates: {summary['earliest_created']} to {summary['latest_created']}")
        if summary['earliest_processed'] and summary['latest_processed']:
            print(f"  • Processing dates: {summary['earliest_processed']} to {summary['latest_processed']}")
    
    # Processing flags summary
    if flags_summary:
        print(f"\n🔧 PROCESSING FLAGS SUMMARY:")
        for flag, data in flags_summary.items():
            if flag != 'total_records' and data is not None:
                print(f"  • {flag.replace('_', ' ').title()}: {data['count']:,} ({data['percentage']:.1f}%)")
    
    # Data completeness summary
    if completeness_summary:
        print(f"\n📋 DATA COMPLETENESS SUMMARY:")
        for status, data in completeness_summary.items():
            if data is not None:
                print(f"  • {status.title()}: {data['count']:,} ({data['percentage']:.1f}%)")
    
    # Data year summary
    if year_summary:
        print(f"\n📅 DATA YEAR BREAKDOWN:")
        for year, data in year_summary.items():
            if data is not None:
                print(f"  • {year}: {data['total_records']:,} records, {data['unique_schools']:,} schools")
                print(f"    - Complete: {data['complete_count']:,}, Enrollment: {data['enrollment_processed']:,}, Demographics: {data['demographics_processed']:,}")
    
    # EDC schools analysis
    if edc_analysis:
        print(f"\n🎯 EDC SCHOOLS ANALYSIS:")
        print(f"  • Schools in CSV: {edc_analysis['total_edc_schools_in_csv']:,}")
        print(f"  • Matched in database: {edc_analysis['matched_schools_in_db']:,}")
        print(f"  • With processing status: {edc_analysis['unique_schools_with_status']:,}")
        print(f"  • Total status records: {edc_analysis['total_status_records']:,}")
        
        print(f"\n  🔧 EDC Processing Flags:")
        for flag, data in edc_analysis['processing_flags'].items():
            if data is not None:
                print(f"    • {flag.replace('_', ' ').title()}: {data['count']:,} ({data['percentage']:.1f}%)")
        
        print(f"\n  📋 EDC Data Completeness:")
        for status, data in edc_analysis['data_completeness'].items():
            if data is not None:
                print(f"    • {status.title()}: {data['count']:,} ({data['percentage']:.1f}%)")

def main():
    global proxy_process
    
    try:
        print("🔍 Starting Processing Status Analysis...")
        
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
        
        # Load EDC schools
        print("\n📥 Loading EDC schools...")
        edc_schools = load_edc_schools()
        
        # Get analysis data
        print("\n📊 Analyzing processing status...")
        summary = get_processing_status_summary(engine)
        flags_summary = get_processing_flags_summary(engine)
        completeness_summary = get_data_completeness_summary(engine)
        year_summary = get_data_year_summary(engine)
        edc_analysis = get_edc_schools_analysis(engine, edc_schools) if edc_schools else None
        
        # Print report
        print_analysis_report(summary, flags_summary, completeness_summary, year_summary, edc_analysis)
        
        # Export detailed analysis
        print("\n📄 Exporting detailed analysis...")
        full_path, edc_path = export_detailed_analysis(engine, edc_schools)
        
        print(f"\n✅ Analysis completed successfully!")
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