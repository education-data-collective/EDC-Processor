#!/usr/bin/env python3
"""
Unified Metrics Calculator

This module calculates school-level metrics using the unified edc_unified database schema.
Generates metrics for multiple drive times (5, 10, 15 minutes) with composite primary key (school_id, drive_time).
Updated to support multiple catchment area analyses per school location.
"""

import os
import sys
import subprocess
import time
import signal
import socket
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.dialects.postgresql import insert
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy")
    sys.exit(1)

# Add projections to path for projection calculations
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'projections'))

from .utils import (
    calculate_grade_filtered_population,
    calculate_enrollment,
    get_school_grades,
    calculate_market_share,
    calculate_percent_change,
    get_status,
    check_newer_school,
    validate_ncessch,
    POPULATION_THRESHOLD,
    PROJECTION_THRESHOLD,
    MARKET_SHARE_THRESHOLD,
    ENROLLMENT_THRESHOLD
)

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = './etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# Drive time constants
DRIVE_TIMES = [5, 10, 15]  # Minutes for catchment area analysis

# Set up logging
logger = logging.getLogger(__name__)

# Global variables for cleanup
proxy_process = None

def signal_handler(signum, frame):
    """Handle cleanup on interruption"""
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
    
    logger.info(f"Starting Cloud SQL Proxy on port {port}")
    
    proxy_process = subprocess.Popen(proxy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(5)
    
    if proxy_process.poll() is not None:
        _, stderr = proxy_process.communicate()
        raise Exception(f"Cloud SQL Proxy failed to start: {stderr.decode()}")
    
    logger.info("Cloud SQL Proxy started successfully")
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

def create_database_connection():
    """Create database connection with proxy"""
    global proxy_process
    
    try:
        proxy_process, port = start_cloud_sql_proxy()
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("Database connection established successfully")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create database connection: {str(e)}")
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)
        raise

def fetch_school_data(engine, limit: Optional[int] = None) -> List[Dict]:
    """Fetch school data from unified database"""
    query = """
    SELECT DISTINCT
        s.id as school_id,
        s.uuid as school_uuid,
        sd.ncessch,
        sd.lea_name,
        sd.state_abbr,
        sn.display_name as school_name,
        sl.location_id,
        lp.latitude,
        lp.longitude
    FROM schools s
    JOIN school_directory sd ON s.id = sd.school_id 
    JOIN school_names sn ON s.id = sn.school_id
    JOIN school_locations sl ON s.id = sl.school_id
    JOIN location_points lp ON sl.location_id = lp.id
    WHERE sd.is_current = true
    AND sn.is_active = true
    AND sl.is_current = true
    AND sd.ncessch IS NOT NULL
    AND sd.ncessch != ''
    ORDER BY sd.ncessch
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        schools = []
        for row in result:
            schools.append({
                'school_id': row[0],
                'school_uuid': row[1],
                'ncessch': row[2],
                'lea_name': row[3],
                'state_abbr': row[4],
                'school_name': row[5],
                'location_id': row[6],
                'latitude': row[7],
                'longitude': row[8]
            })
        return schools

def fetch_enrollment_data(engine, school_ids: List[int]) -> Dict[int, Dict]:
    """Fetch enrollment data for given school IDs"""
    if not school_ids:
        return {}
        
    # Create placeholder list for SQL IN clause
    placeholders = ','.join([f':school_id_{i}' for i in range(len(school_ids))])
    params = {f'school_id_{i}': school_id for i, school_id in enumerate(school_ids)}
    
    query = f"""
    SELECT 
        school_id,
        data_year,
        grade,
        total
    FROM school_enrollments
    WHERE school_id IN ({placeholders})
    AND data_year IN (2019, 2021)
    AND total >= 0
    ORDER BY school_id, data_year, grade
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        
        enrollment_data = {}
        for row in result:
            school_id, data_year, grade, total = row
            if school_id not in enrollment_data:
                enrollment_data[school_id] = {'current': {}, 'comparison': {}}
            
            # Map data years to current/comparison
            year_key = 'current' if data_year == 2021 else 'comparison'
            
            # Map grade format
            if grade == 'KG':
                grade_key = 'Kindergarten'
            elif grade.isdigit():
                grade_key = f'Grade {grade}'
            else:
                grade_key = grade
                
            enrollment_data[school_id][year_key][grade_key] = total
            
        return enrollment_data

def fetch_esri_data(engine, location_ids: List[int]) -> Dict[int, Dict[int, Dict]]:
    """Fetch ESRI demographic data for given location IDs and all drive times"""
    if not location_ids:
        return {}
    
    # Create placeholder list for SQL IN clause
    placeholders = ','.join([f':location_id_{i}' for i in range(len(location_ids))])
    params = {f'location_id_{i}': location_id for i, location_id in enumerate(location_ids)}
    
    # Create drive time placeholders
    drive_time_placeholders = ','.join([f':drive_time_{i}' for i in range(len(DRIVE_TIMES))])
    params.update({f'drive_time_{i}': drive_time for i, drive_time in enumerate(DRIVE_TIMES)})
    
    query = f"""
    SELECT 
        location_id,
        drive_time,
        age4_cy, age5_cy, age6_cy, age7_cy, age8_cy, age9_cy, age10_cy,
        age11_cy, age12_cy, age13_cy, age14_cy, age15_cy, age16_cy, age17_cy,
        age4_fy, age5_fy, age6_fy, age7_fy, age8_fy, age9_fy, age10_fy,
        age11_fy, age12_fy, age13_fy, age14_fy, age15_fy, age16_fy, age17_fy,
        age4_c20, age5_c20, age6_c20, age7_c20, age8_c20, age9_c20, age10_c20,
        age11_c20, age12_c20, age13_c20, age14_c20, age15_c20, age16_c20, age17_c20
    FROM esri_demographic_data
    WHERE location_id IN ({placeholders})
    AND drive_time IN ({drive_time_placeholders})
    ORDER BY location_id, drive_time
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        
        esri_data = {}
        for row in result:
            location_id = row[0]
            drive_time = row[1]
            
            # Build age arrays
            ages_current = [row[i] for i in range(2, 16)]  # age4_cy through age17_cy
            ages_future = [row[i] for i in range(16, 30)]  # age4_fy through age17_fy
            ages_2020 = [row[i] for i in range(30, 44)]    # age4_c20 through age17_c20
            
            # Structure data by location_id -> drive_time -> age data
            if location_id not in esri_data:
                esri_data[location_id] = {}
            
            esri_data[location_id][drive_time] = {
                'ages': {
                    '4_17': {
                        'current': ages_current,
                        'future': ages_future,
                        '2020': ages_2020
                    }
                }
            }
            
        return esri_data

def get_school_projections_from_database(engine, school_id: int) -> Dict[str, Any]:
    """Get school projections from database (placeholder for now)"""
    # Since school_projections table is empty, return default values
    # This will be updated when projections are populated
    return {
        'has_projections': False,
        'projection_type': 'none',
        'public_projected': 0,
        'updated_projected': 0
    }

def calculate_metrics_for_school(school: Dict, enrollment_data: Dict, esri_data: Dict, projections: Dict, drive_time: int) -> Optional[Dict]:
    """Calculate all metrics for a single school at a specific drive time"""
    try:
        # Validate NCESSCH
        validated_ncessch = validate_ncessch(school['ncessch'])
        
        # Get school data
        school_id = school['school_id']
        location_id = school['location_id']
        
        school_enrollment = enrollment_data.get(school_id, {'current': {}, 'comparison': {}})
        
        # Get ESRI data for the specific drive time
        location_esri_data = esri_data.get(location_id, {})
        school_esri = location_esri_data.get(drive_time, {})
        
        # Get current grades
        current_grades = get_school_grades({'enrollment_by_grade': school_enrollment})
        
        if not current_grades:
            logger.warning(f"No grades found for school {validated_ncessch} at {drive_time}min drive time")
            return None
        
        # Calculate population metrics
        pop_totals = calculate_grade_filtered_population(school_esri, current_grades)
        pop_trends = {
            'past_to_latest': calculate_percent_change(pop_totals['current'], pop_totals['past']),
            'latest_to_projected': calculate_percent_change(pop_totals['future'], pop_totals['current'])
        }
        
        # Calculate enrollment metrics
        enrollments = {
            'current': calculate_enrollment(school_enrollment.get('current', {}), current_grades),
            'past': calculate_enrollment(school_enrollment.get('comparison', {}), current_grades)
        }
        
        # Calculate market shares
        market_shares = {
            'current': calculate_market_share(enrollments['current'], pop_totals['current']),
            'past': calculate_market_share(enrollments['past'], pop_totals['past'])
        }
        
        # Calculate trends
        enrollment_trend_past_to_latest = calculate_percent_change(
            enrollments['current'], 
            enrollments['past']
        )
        
        # Calculate projected enrollment trend
        projected_enrollment = (projections['updated_projected'] 
                               if projections['projection_type'] == 'updated' 
                               else projections['public_projected'])
        
        enrollment_trend_latest_to_projected = calculate_percent_change(
            projected_enrollment,
            enrollments['current']
        )
        
        # Create metrics record
        metrics = {
            'school_id': school_id,
            'drive_time': drive_time,  # Add drive_time to the metrics
            'ncessch': validated_ncessch,  # Keep for reference
            'calculated_at': datetime.utcnow(),
            'data_versions': {
                'membership_data_year': 2021,
                'comparison_year': 2019,
                'esri_data_year': 2021,
                'drive_time_minutes': drive_time,
                'processed_at': datetime.utcnow().isoformat()
            },
            'population_past': int(pop_totals['past']),
            'population_current': int(pop_totals['current']),
            'population_future': int(pop_totals['future']),
            'population_trend_past_to_latest': min(max(round(pop_trends['past_to_latest'], 2), -999.99), 999.99),
            'population_trend_latest_to_projected': min(max(round(pop_trends['latest_to_projected'], 2), -999.99), 999.99),
            'population_trend_status': get_status(pop_trends['past_to_latest'], POPULATION_THRESHOLD),
            'population_projection_status': get_status(pop_trends['latest_to_projected'], PROJECTION_THRESHOLD),
            'market_share_past': min(round(market_shares['past'], 2), 999.99),
            'market_share_current': min(round(market_shares['current'], 2), 999.99),
            'market_share_trend': min(max(round(market_shares['current'] - market_shares['past'], 2), -999.99), 999.99),
            'market_share_status': get_status(
                market_shares['current'] - market_shares['past'], 
                MARKET_SHARE_THRESHOLD, 
                'market_share'
            ),
            'enrollment_past': int(enrollments['past']),
            'enrollment_current': int(enrollments['current']),
            'public_enrollment_projected': int(projections['public_projected']),
            'updated_enrollment_projected': int(projections['updated_projected']),
            'projection_type': projections['projection_type'],
            'enrollment_trend_past_to_latest': min(max(round(enrollment_trend_past_to_latest, 2), -999.99), 999.99),
            'enrollment_trend_latest_to_projected': min(max(round(enrollment_trend_latest_to_projected, 2), -999.99), 999.99),
            'enrollment_trend_status': get_status(enrollment_trend_past_to_latest, ENROLLMENT_THRESHOLD),
            'enrollment_projection_status': get_status(enrollment_trend_latest_to_projected, PROJECTION_THRESHOLD),
            'is_newer': check_newer_school(school_enrollment),
            'has_projections': projections['has_projections']
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating metrics for school {school.get('ncessch', 'unknown')} at {drive_time}min: {str(e)}")
        return None

def save_metrics_to_database(engine, metrics_list: List[Dict], table_name: str = 'school_metrics') -> Tuple[int, int]:
    """Save metrics to database using upsert with composite primary key (school_id, drive_time)"""
    success_count = 0
    error_count = 0
    
    for metrics in metrics_list:
        try:
            # Remove SQLAlchemy internal attributes and prepare for insert
            clean_metrics = {k: v for k, v in metrics.items() if not k.startswith('_')}
            
            # Handle JSON fields
            if 'data_versions' in clean_metrics:
                import json
                clean_metrics['data_versions'] = json.dumps(clean_metrics['data_versions'])
            
            # Use raw SQL for upsert since PostgreSQL-specific syntax is complex
            upsert_sql = f"""
            INSERT INTO {table_name} (
                school_id, drive_time, ncessch, calculated_at, data_versions,
                population_past, population_current, population_future,
                population_trend_past_to_latest, population_trend_latest_to_projected,
                population_trend_status, population_projection_status,
                market_share_past, market_share_current, market_share_trend, market_share_status,
                enrollment_past, enrollment_current, 
                public_enrollment_projected, updated_enrollment_projected, projection_type,
                enrollment_trend_past_to_latest, enrollment_trend_latest_to_projected,
                enrollment_trend_status, enrollment_projection_status,
                is_newer, has_projections
            ) VALUES (
                :school_id, :drive_time, :ncessch, :calculated_at, :data_versions,
                :population_past, :population_current, :population_future,
                :population_trend_past_to_latest, :population_trend_latest_to_projected,
                :population_trend_status, :population_projection_status,
                :market_share_past, :market_share_current, :market_share_trend, :market_share_status,
                :enrollment_past, :enrollment_current,
                :public_enrollment_projected, :updated_enrollment_projected, :projection_type,
                :enrollment_trend_past_to_latest, :enrollment_trend_latest_to_projected,
                :enrollment_trend_status, :enrollment_projection_status,
                :is_newer, :has_projections
            )
            ON CONFLICT (school_id, drive_time) DO UPDATE SET
                ncessch = EXCLUDED.ncessch,
                calculated_at = EXCLUDED.calculated_at,
                data_versions = EXCLUDED.data_versions,
                population_past = EXCLUDED.population_past,
                population_current = EXCLUDED.population_current,
                population_future = EXCLUDED.population_future,
                population_trend_past_to_latest = EXCLUDED.population_trend_past_to_latest,
                population_trend_latest_to_projected = EXCLUDED.population_trend_latest_to_projected,
                population_trend_status = EXCLUDED.population_trend_status,
                population_projection_status = EXCLUDED.population_projection_status,
                market_share_past = EXCLUDED.market_share_past,
                market_share_current = EXCLUDED.market_share_current,
                market_share_trend = EXCLUDED.market_share_trend,
                market_share_status = EXCLUDED.market_share_status,
                enrollment_past = EXCLUDED.enrollment_past,
                enrollment_current = EXCLUDED.enrollment_current,
                public_enrollment_projected = EXCLUDED.public_enrollment_projected,
                updated_enrollment_projected = EXCLUDED.updated_enrollment_projected,
                projection_type = EXCLUDED.projection_type,
                enrollment_trend_past_to_latest = EXCLUDED.enrollment_trend_past_to_latest,
                enrollment_trend_latest_to_projected = EXCLUDED.enrollment_trend_latest_to_projected,
                enrollment_trend_status = EXCLUDED.enrollment_trend_status,
                enrollment_projection_status = EXCLUDED.enrollment_projection_status,
                is_newer = EXCLUDED.is_newer,
                has_projections = EXCLUDED.has_projections
            """
            
            # Execute the upsert
            with engine.begin() as conn:
                conn.execute(text(upsert_sql), clean_metrics)
                success_count += 1
                
        except Exception as e:
            logger.error(f"Error saving metrics for school_id {metrics.get('school_id', 'unknown')} at {metrics.get('drive_time', 'unknown')}min: {str(e)}")
            error_count += 1
    
    return success_count, error_count

def export_metrics_to_csv(metrics_list: List[Dict], filename: str) -> bool:
    """Export metrics to CSV file"""
    if not metrics_list:
        logger.warning("No metrics to export to CSV")
        return False
    
    try:
        # Define CSV headers based on metrics structure
        headers = [
            'school_id', 'drive_time', 'ncessch', 'calculated_at',
            'population_past', 'population_current', 'population_future',
            'population_trend_past_to_latest', 'population_trend_latest_to_projected',
            'population_trend_status', 'population_projection_status',
            'market_share_past', 'market_share_current', 'market_share_trend', 'market_share_status',
            'enrollment_past', 'enrollment_current',
            'public_enrollment_projected', 'updated_enrollment_projected', 'projection_type',
            'enrollment_trend_past_to_latest', 'enrollment_trend_latest_to_projected',
            'enrollment_trend_status', 'enrollment_projection_status',
            'is_newer', 'has_projections',
            'membership_data_year', 'comparison_year', 'esri_data_year', 'drive_time_minutes'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for metrics in metrics_list:
                # Flatten the data_versions into the main record
                row = {}
                for header in headers:
                    if header in ['membership_data_year', 'comparison_year', 'esri_data_year', 'drive_time_minutes']:
                        # Extract from data_versions
                        data_versions = metrics.get('data_versions', {})
                        if isinstance(data_versions, str):
                            import json
                            data_versions = json.loads(data_versions)
                        row[header] = data_versions.get(header, '')
                    else:
                        row[header] = metrics.get(header, '')
                
                writer.writerow(row)
        
        logger.info(f"Successfully exported {len(metrics_list)} metrics to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting metrics to CSV: {str(e)}")
        return False

def calculate_unified_metrics(limit: Optional[int] = None, table_name: str = 'school_metrics', export_csv: Optional[str] = None) -> Dict[str, int]:
    """Main function to calculate metrics using unified database"""
    global proxy_process
    
    try:
        logger.info("Starting unified metrics calculation for multiple drive times")
        logger.info(f"Drive times to process: {DRIVE_TIMES} minutes")
        
        # Create database connection
        engine = create_database_connection()
        
        # Fetch school data
        logger.info("Fetching school data...")
        schools = fetch_school_data(engine, limit)
        logger.info(f"Found {len(schools)} schools to process")
        
        if not schools:
            logger.warning("No schools found to process")
            return {'total_schools': 0, 'total_metrics': 0, 'success': 0, 'errors': 0}
        
        # Extract IDs for batch queries
        school_ids = [school['school_id'] for school in schools]
        location_ids = list(set(school['location_id'] for school in schools))
        
        # Fetch enrollment data
        logger.info("Fetching enrollment data...")
        enrollment_data = fetch_enrollment_data(engine, school_ids)
        
        # Fetch ESRI data for all drive times
        logger.info(f"Fetching ESRI demographic data for drive times: {DRIVE_TIMES}")
        esri_data = fetch_esri_data(engine, location_ids)
        
        # Calculate metrics for each school and drive time combination
        logger.info("Calculating metrics for all school-drive time combinations...")
        metrics_list = []
        total_metrics_expected = len(schools) * len(DRIVE_TIMES)
        
        for i, school in enumerate(schools, 1):
            # Get projections (placeholder for now)
            projections = get_school_projections_from_database(engine, school['school_id'])
            
            # Calculate metrics for each drive time
            for drive_time in DRIVE_TIMES:
                metrics = calculate_metrics_for_school(school, enrollment_data, esri_data, projections, drive_time)
                
                if metrics:
                    metrics_list.append(metrics)
            
            if i % 50 == 0:  # Log progress every 50 schools
                current_metrics = len(metrics_list)
                logger.info(f"Processed {i}/{len(schools)} schools ({current_metrics}/{total_metrics_expected} metrics)")
        
        # Save metrics to database
        logger.info(f"Saving {len(metrics_list)} metrics records to database...")
        success_count, error_count = save_metrics_to_database(engine, metrics_list, table_name)
        
        # Export metrics to CSV
        if export_csv:
            logger.info(f"Exporting metrics to CSV file: {export_csv}")
            export_metrics_to_csv(metrics_list, export_csv)
        
        results = {
            'total_schools': len(schools),
            'total_metrics': len(metrics_list),
            'expected_metrics': total_metrics_expected,
            'success': success_count,
            'errors': error_count,
            'drive_times_processed': DRIVE_TIMES
        }
        
        logger.info(f"Metrics calculation complete: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in unified metrics calculation: {str(e)}")
        raise
    finally:
        # Cleanup
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Calculate school metrics using unified database')
    parser.add_argument('--limit', type=int, help='Limit number of schools to process (for testing)')
    parser.add_argument('--table', type=str, default='school_metrics', help='Table name for metrics storage')
    parser.add_argument('--export', type=str, help='CSV file to export metrics to')
    args = parser.parse_args()
    
    try:
        results = calculate_unified_metrics(limit=args.limit, table_name=args.table, export_csv=args.export)
        print(f"Results: {results}")
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1) 