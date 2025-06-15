#!/usr/bin/env python3
"""
Enrollment Projections CSV Generator

This script:
1. Connects to the database
2. Fetches school enrollment data
3. Generates projections using the enrollment_projections engine
4. Exports results to CSV files for review
"""

import os
import sys
import subprocess
import time
import signal
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add enrollment_projections to path
sys.path.insert(0, str(Path(__file__).parent / "enrollment_projections"))

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = './etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

try:
    from sqlalchemy import create_engine, text
except ImportError:
    print("SQLAlchemy not found. Please install it with: pip install sqlalchemy pandas")
    sys.exit(1)

# Standalone projection logic (simplified from enrollment_projections package)
import statistics as stats

# Type aliases from data_structures
SchoolData = Dict[str, Any]

# Constants from utils.py
GRADE_MAP: Dict[str, int] = {
    'Kindergarten': 0,
    'Grade 1': 1,
    'Grade 2': 2,
    'Grade 3': 3,
    'Grade 4': 4,
    'Grade 5': 5,
    'Grade 6': 6,
    'Grade 7': 7,
    'Grade 8': 8,
    'Grade 9': 9,
    'Grade 10': 10,
    'Grade 11': 11,
    'Grade 12': 12
}

PREVIOUS_GRADE_MAP = {
    'Grade 1': 'Kindergarten',
    'Grade 2': 'Grade 1',
    'Grade 3': 'Grade 2',
    'Grade 4': 'Grade 3',
    'Grade 5': 'Grade 4',
    'Grade 6': 'Grade 5',
    'Grade 7': 'Grade 6',
    'Grade 8': 'Grade 7',
    'Grade 9': 'Grade 8',
    'Grade 10': 'Grade 9',
    'Grade 11': 'Grade 10',
    'Grade 12': 'Grade 11'
}

def generate_forecast_years(most_recent_year: str, num_years: int = 5) -> List[str]:
    start_year = int(most_recent_year.split('-')[0]) + 1
    return [f"{year}-{year+1}" for year in range(start_year, start_year + num_years)]

def get_most_recent_year(enrollment_data: Dict[str, Dict]) -> str:
    return max(enrollment_data.keys())

# Simplified projection functions
def calculate_survival_rates(school_data: SchoolData, grade_map: Dict[str, int]) -> SchoolData:
    available_years = sorted([year for year in school_data['enrollment'].keys() if year is not None], reverse=True)
    if not available_years:
        print(f"Skipping school: {school_data['id']} because it does not have any enrollment data")
        return school_data

    # Get discontinued grades from latest year (enrollment = -1)
    latest_year = available_years[0]
    discontinued_grades = {
        grade for grade, enrollment in school_data['enrollment'][latest_year].items() 
        if enrollment == -1
    }

    grades_to_analyze = sorted([
        grade for grade in school_data['enrollment'][available_years[0]].keys() 
        if isinstance(school_data['enrollment'][available_years[0]][grade], (int, float)) 
        and school_data['enrollment'][available_years[0]][grade] >= 0
    ], key=lambda x: grade_map.get(x, float('inf')))

    if not grades_to_analyze:
        print(f"Skipping school: {school_data['id']} because it does not have any enrollment data for the most recent year: {available_years[0]}")
        return school_data

    entry_grade = min(grades_to_analyze, key=lambda x: grade_map.get(x, float('inf')))
    school_data['entryGrade'] = entry_grade

    survival_rates = {}
    historical_patterns = {}

    # Store historical enrollment patterns for each grade
    for grade in grades_to_analyze:
        historical_enrollments = [
            school_data['enrollment'][year][grade] 
            for year in available_years 
            if grade in school_data['enrollment'][year] 
            and school_data['enrollment'][year][grade] > 0
        ]
        if historical_enrollments:
            historical_patterns[grade] = {
                'min': min(historical_enrollments),
                'max': max(historical_enrollments),
                'median': stats.median(historical_enrollments)
            }

    # 1-year survival rates
    if len(available_years) >= 2:
        survival_rates['oneYear'] = {}
        current_year = available_years[0]
        previous_year = available_years[1]
        for grade in grades_to_analyze:
            if grade == entry_grade or grade in discontinued_grades:
                continue
            previous_grade = PREVIOUS_GRADE_MAP.get(grade)
            if previous_grade and previous_grade not in discontinued_grades:
                current_enrollment = school_data['enrollment'][current_year].get(grade, 0)
                previous_enrollment = school_data['enrollment'][previous_year].get(previous_grade, 0)
                if previous_enrollment > 0 and current_enrollment >= 0:
                    survival_rates['oneYear'][grade] = current_enrollment / previous_enrollment

    # 3-year survival rates
    if len(available_years) >= 4:
        survival_rates['threeYear'] = {}
        for grade in grades_to_analyze:
            if grade == entry_grade or grade in discontinued_grades:
                continue
            previous_grade = PREVIOUS_GRADE_MAP.get(grade)
            if previous_grade and previous_grade not in discontinued_grades:
                current_sum = sum(enrollment for year in available_years[:3]
                                for enrollment in [school_data['enrollment'].get(year, {}).get(grade, 0)]
                                if enrollment >= 0)
                previous_sum = sum(enrollment for year in available_years[1:4]
                                 for enrollment in [school_data['enrollment'].get(year, {}).get(previous_grade, 0)]
                                 if enrollment >= 0)
                if previous_sum > 0:
                    survival_rates['threeYear'][grade] = current_sum / previous_sum

    # 5-year survival rates
    if len(available_years) >= 6:
        survival_rates['fiveYear'] = {}
        for grade in grades_to_analyze:
            if grade == entry_grade or grade in discontinued_grades:
                continue
            previous_grade = PREVIOUS_GRADE_MAP.get(grade)
            if previous_grade and previous_grade not in discontinued_grades:
                current_sum = sum(enrollment for year in available_years[:5]
                                for enrollment in [school_data['enrollment'].get(year, {}).get(grade, 0)]
                                if enrollment >= 0)
                previous_sum = sum(enrollment for year in available_years[1:6]
                                 for enrollment in [school_data['enrollment'].get(year, {}).get(previous_grade, 0)]
                                 if enrollment >= 0)
                if previous_sum > 0:
                    survival_rates['fiveYear'][grade] = current_sum / previous_sum

    school_data['survivalRates'] = survival_rates
    school_data['historicalPatterns'] = historical_patterns
    school_data['discontinuedGrades'] = discontinued_grades
    print(f"Survival rates calculated for school: {school_data['id']}")
    return school_data

def calculate_outer_max_min(school_data: SchoolData) -> SchoolData:
    """Calculate outer bounds (historical min/max) for each grade"""
    outer_values = {}
    
    available_years = sorted(school_data['enrollment'].keys(), reverse=True)
    if not available_years:
        print(f"Warning: No enrollment data found for school {school_data['id']}")
        return school_data
    
    for grade in school_data['enrollment'][available_years[0]].keys():
        enrollments = []
        for year in available_years:
            if grade in school_data['enrollment'][year]:
                enrollment = school_data['enrollment'][year][grade]
                if isinstance(enrollment, (int, float)) and enrollment >= 0:
                    enrollments.append(enrollment)
            
        if enrollments:
            outer_values[grade] = {
                "outer_max": max(enrollments),
                "outer_min": min(enrollments)
            }
        else:
            outer_values[grade] = {
                "outer_max": 0,
                "outer_min": 0
            }
    
    school_data['outerValues'] = outer_values
    return school_data

def calculate_entry_grade_estimates(school_data: SchoolData, grade_map: Dict[str, int]) -> SchoolData:
    print(f"\nProcessing school: {school_data['id']} ({school_data.get('school_name', 'Unknown')})")
    
    available_years = sorted([year for year in school_data['enrollment'].keys() if year is not None], reverse=True)
    if not available_years:
        print(f"Skipping school: {school_data['id']} because no enrollment data is available")
        return school_data

    most_recent_year = available_years[0]
    grades = school_data['enrollment'][most_recent_year].keys()
    
    # Determine entry grade - if Pre-K is present, set entry grade to Kindergarten
    if 'Pre-Kindergarten' in grades:
        entry_grade = 'Kindergarten'
    else:
        entry_grade = min(grades, key=lambda x: grade_map.get(x, float('inf')))
    
    school_data['entryGrade'] = entry_grade

    # Calculate averages for different time periods
    entry_grade_averages = {'oneYear': 0, 'threeYear': 0, 'fiveYear': 0}
    entry_grade_enrollments = []
    
    for period, years in [('oneYear', 1), ('threeYear', 3), ('fiveYear', 5)]:
        total = 0
        count = 0
        for year in available_years[:years]:
            if entry_grade in school_data['enrollment'][year]:
                enrollment = school_data['enrollment'][year][entry_grade]
                if enrollment is not None and enrollment > 0:
                    total += enrollment
                    count += 1
                    entry_grade_enrollments.append(enrollment)
        if count > 0:
            entry_grade_averages[period] = total / count

    # Get the actual average values (filtering out zeros)
    averages = [v for v in entry_grade_averages.values() if v > 0]
    
    if averages:
        entry_grade_estimates = {
            "low": round(min(averages)),
            "high": round(max(averages)),
            "median": round(sorted(averages)[len(averages)//2]),
            "outer_min": round(min(entry_grade_enrollments)) if entry_grade_enrollments else 0,
            "outer_max": round(max(entry_grade_enrollments)) if entry_grade_enrollments else 0
        }
    else:
        entry_grade_estimates = {
            "low": 0,
            "high": 0,
            "median": 0,
            "outer_min": 0,
            "outer_max": 0
        }
    
    school_data['entryGradeEstimates'] = entry_grade_estimates
    return school_data

def calculate_forecast_survival_rates(school_data: SchoolData) -> SchoolData:
    """Calculate forecast survival rates using multiple time periods"""
    school_data = calculate_outer_max_min(school_data)
    
    survival_rates = school_data.get('survivalRates', {})
    outer_values = school_data.get('outerValues', {})
    discontinued_grades = school_data.get('discontinuedGrades', set())
    forecast_survival_rates = {}
    
    # Only process active grades (not discontinued)
    for grade in outer_values.keys():
        if grade in discontinued_grades:
            continue
            
        one_year_rate = survival_rates.get('oneYear', {}).get(grade, 0)
        three_year_rate = survival_rates.get('threeYear', {}).get(grade, 0)
        five_year_rate = survival_rates.get('fiveYear', {}).get(grade, 0)
        
        # If no rates available, use historical retention patterns
        if not any([one_year_rate, three_year_rate, five_year_rate]):
            historical_enrollments = [
                val for year in school_data['enrollment']
                for val in [school_data['enrollment'][year].get(grade)]
                if val and val > 0
            ]
            if historical_enrollments:
                min_enrollment = min(historical_enrollments)
                max_enrollment = max(historical_enrollments)
                median_enrollment = stats.median(historical_enrollments)
                forecast_survival_rates[grade] = {
                    "median": 1.0,  # Maintain current enrollment
                    "min": min_enrollment / median_enrollment if median_enrollment > 0 else 0.9,
                    "max": max_enrollment / median_enrollment if median_enrollment > 0 else 1.1,
                    "outer_max": outer_values[grade]["outer_max"],
                    "outer_min": outer_values[grade]["outer_min"]
                }
                continue
        
        # If 3-year rate is missing, use 1-year rate
        if three_year_rate == 0 and one_year_rate != 0:
            three_year_rate = one_year_rate
        
        # If 5-year rate is missing, use average of available rates
        if five_year_rate == 0:
            available_rates = [r for r in [one_year_rate, three_year_rate] if r != 0]
            five_year_rate = sum(available_rates) / len(available_rates) if available_rates else 0
        
        rates = [one_year_rate, three_year_rate, five_year_rate]
        non_zero_rates = [r for r in rates if r != 0]
        
        if non_zero_rates:
            forecast_survival_rates[grade] = {
                "median": stats.median(non_zero_rates),
                "min": min(non_zero_rates),
                "max": max(non_zero_rates),
                "outer_max": outer_values[grade]["outer_max"],
                "outer_min": outer_values[grade]["outer_min"]
            }
        else:
            # Default fallback values
            forecast_survival_rates[grade] = {
                "median": 1.0,  # Default to maintaining current enrollment
                "min": 0.9,     # Small decrease
                "max": 1.1,     # Small increase
                "outer_max": outer_values[grade]["outer_max"],
                "outer_min": outer_values[grade]["outer_min"]
            }
    
    school_data['forecastSurvivalRates'] = forecast_survival_rates
    return school_data

def generate_projections(school_data: SchoolData, grade_map: Dict[str, int], forecast_years: List[str]) -> SchoolData:
    """Generate enrollment projections with proper edge case handling"""
    enrollment = school_data['enrollment']
    forecast_survival_rates = school_data.get('forecastSurvivalRates', {})
    outer_values = school_data.get('outerValues', {})
    discontinued_grades = school_data.get('discontinuedGrades', set())

    available_years = sorted([year for year in enrollment.keys() if year is not None], reverse=True)
    if not available_years:
        print(f"Skipping projections for school {school_data['id']}: No enrollment data available")
        return school_data
    
    latest_year = available_years[0]

    # Get all active grades (excluding discontinued ones and Pre-Kindergarten)
    active_grades = [grade for grade, value in enrollment[latest_year].items() 
                    if value != -1 and grade != 'Pre-Kindergarten' and grade not in discontinued_grades]
    
    if not active_grades:
        print(f"No active grades found for school {school_data['id']}")
        return school_data
        
    # Sort active grades by grade level
    latest_grades = sorted(active_grades, key=lambda x: grade_map.get(x, float('inf')))
    
    projections = {
        'min': {}, 'median': {}, 'max': {}, 'outer_min': {}, 'outer_max': {}
    }
    
    # Determine entry grade - if Kindergarten exists, use it; otherwise use lowest grade
    if 'Kindergarten' in active_grades:
        entry_grade = 'Kindergarten'
    else:
        entry_grade = min(active_grades, key=lambda x: grade_map.get(x, float('inf')))
    
    school_data['entryGrade'] = entry_grade
    entry_grade_estimates = school_data.get('entryGradeEstimates', {})
    
    for year in forecast_years:
        for projection_type in ['min', 'median', 'max']:
            projections[projection_type][year] = {}
            
            # Process each active grade
            for grade in latest_grades:
                if grade == entry_grade:
                    # Use entry grade estimates, ensuring we don't use negative values
                    if projection_type == 'min':
                        value = max(0, entry_grade_estimates.get('low', 0))
                    elif projection_type == 'max':
                        value = max(0, entry_grade_estimates.get('high', 0))
                    else:  # median
                        value = max(0, entry_grade_estimates.get('median', 0))
                else:
                    if grade not in forecast_survival_rates:
                        # Use historical patterns as fallback
                        historical_patterns = school_data.get('historicalPatterns', {}).get(grade, {})
                        if historical_patterns:
                            if projection_type == 'min':
                                value = max(0, historical_patterns['min'])
                            elif projection_type == 'max':
                                value = max(0, historical_patterns['max'])
                            else:
                                value = max(0, historical_patterns['median'])
                        else:
                            last_actual = enrollment[latest_year].get(grade, 0)
                            value = max(0, last_actual) if last_actual is not None else 0
                        projections[projection_type][year][grade] = round(value)
                        continue
                            
                    rate = forecast_survival_rates.get(grade, {}).get(projection_type, 1)
                    
                    # Find previous active grade
                    grade_index = latest_grades.index(grade)
                    previous_grade = None
                    for prev_grade in reversed(latest_grades[:grade_index]):
                        if prev_grade in active_grades:
                            previous_grade = prev_grade
                            break
                    
                    if not previous_grade:
                        # Use historical patterns if no previous grade
                        historical_patterns = school_data.get('historicalPatterns', {}).get(grade, {})
                        if historical_patterns:
                            if projection_type == 'min':
                                value = max(0, historical_patterns['min'])
                            elif projection_type == 'max':
                                value = max(0, historical_patterns['max'])
                            else:
                                value = max(0, historical_patterns['median'])
                        else:
                            last_actual = enrollment[latest_year].get(grade, 0)
                            value = max(0, last_actual) if last_actual is not None else 0
                    else:
                        if year == forecast_years[0]:
                            prev_enrollment = enrollment[latest_year].get(previous_grade, 0)
                            value = max(0, prev_enrollment * rate) if prev_enrollment > 0 else 0
                        else:
                            previous_year = forecast_years[forecast_years.index(year)-1]
                            prev_value = projections[projection_type][previous_year].get(previous_grade, 0)
                            value = max(0, prev_value * rate)
                
                projections[projection_type][year][grade] = round(value)
        
        # Set proper outer bounds using historical data
        projections['outer_min'][year] = {
            grade: max(0, outer_values.get(grade, {}).get('outer_min', 0)) 
            for grade in latest_grades
        }
        projections['outer_max'][year] = {
            grade: max(0, outer_values.get(grade, {}).get('outer_max', 0))
            for grade in latest_grades
        }

    school_data['projections'] = projections
    return school_data

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
    import socket
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
        raise Exception(f"Cloud SQL Proxy failed: {stderr.decode()}")
    
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

def fetch_schools_sample(engine, limit=50):
    """Fetch a sample of schools with directory data"""
    with engine.connect() as conn:
        # Get schools from directory (since enrollment table is empty)
        query = text("""
            SELECT DISTINCT s.id, s.uuid, sd.ncessch, 
                   COALESCE(sd.system_name, 'Unknown School') as school_name
            FROM schools s
            JOIN school_directory sd ON s.id = sd.school_id
            WHERE sd.ncessch IS NOT NULL 
            AND sd.ncessch != ''
            ORDER BY s.id
            LIMIT :limit
        """)
        
        result = conn.execute(query, {"limit": limit})
        schools = []
        
        for row in result:
            schools.append({
                'id': row.id,
                'uuid': row.uuid,
                'ncessch': row.ncessch,
                'school_name': row.school_name
            })
        
        return schools

def fetch_enrollment_data(engine, school_id):
    """Fetch historical enrollment data for a school"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                se.school_year,
                se.grade,
                se.total as total_enrollment
            FROM school_enrollments se
            WHERE se.school_id = :school_id
            AND se.total > 0
            ORDER BY se.school_year, se.grade
        """)
        
        result = conn.execute(query, {"school_id": school_id})
        enrollment_data = {}
        
        for row in result:
            year = row.school_year
            grade = row.grade
            enrollment = row.total_enrollment
            
            if year not in enrollment_data:
                enrollment_data[year] = {}
            
            # Map database grade format to projection engine format
            grade_mapped = map_grade_format(grade)
            if grade_mapped:
                enrollment_data[year][grade_mapped] = enrollment
        
        return enrollment_data

def map_grade_format(db_grade):
    """Map database grade format to projection engine format"""
    grade_mapping = {
        'KG': 'Kindergarten',
        'PK': 'Pre-Kindergarten',
        '01': 'Grade 1',
        '02': 'Grade 2',
        '03': 'Grade 3',
        '04': 'Grade 4',
        '05': 'Grade 5',
        '06': 'Grade 6',
        '07': 'Grade 7',
        '08': 'Grade 8',
        '09': 'Grade 9',
        '10': 'Grade 10',
        '11': 'Grade 11',
        '12': 'Grade 12'
    }
    return grade_mapping.get(db_grade, None)

def generate_school_projections(school_data):
    """Generate projections for a single school using the projection engine"""
    try:
        # Ensure we have enrollment data
        if not school_data.get('enrollment'):
            return None
        
        # Determine the most recent year and generate forecast years
        most_recent_year = get_most_recent_year(school_data['enrollment'])
        forecast_years = generate_forecast_years(most_recent_year)
        
        # Run through the projection pipeline
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        school_data = calculate_forecast_survival_rates(school_data)
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        school_data = generate_projections(school_data, GRADE_MAP, forecast_years)
        
        return school_data
        
    except Exception as e:
        print(f"Error generating projections for school {school_data.get('id', 'unknown')}: {str(e)}")
        return None

def format_projections_for_csv(school_data):
    """Format projection data into rows for CSV export"""
    rows = []
    
    if not school_data or 'projections' not in school_data:
        return rows
    
    school_info = {
        'school_id': school_data['id'],
        'ncessch': school_data['ncessch'],
        'school_name': school_data['school_name'],
        'entry_grade': school_data.get('entryGrade', ''),
    }
    
    projections = school_data['projections']
    
    # Process each projection type and year
    for projection_type in ['min', 'median', 'max', 'outer_min', 'outer_max']:
        if projection_type in projections:
            for year, grades in projections[projection_type].items():
                for grade, enrollment in grades.items():
                    row = {
                        **school_info,
                        'projection_year': year,
                        'projection_type': projection_type,
                        'grade': grade,
                        'projected_enrollment': enrollment,
                        'generated_at': datetime.now().isoformat()
                    }
                    
                    # Add survival rates if available
                    survival_rates = school_data.get('forecastSurvivalRates', {}).get(grade, {})
                    row['survival_rate_min'] = survival_rates.get('min', '')
                    row['survival_rate_median'] = survival_rates.get('median', '')
                    row['survival_rate_max'] = survival_rates.get('max', '')
                    
                    # Add entry grade estimates if this is an entry grade
                    if grade == school_data.get('entryGrade'):
                        entry_estimates = school_data.get('entryGradeEstimates', {})
                        row['entry_grade_low'] = entry_estimates.get('low', '')
                        row['entry_grade_high'] = entry_estimates.get('high', '')
                        row['entry_grade_median'] = entry_estimates.get('median', '')
                    else:
                        row['entry_grade_low'] = ''
                        row['entry_grade_high'] = ''
                        row['entry_grade_median'] = ''
                    
                    rows.append(row)
    
    return rows

def save_to_csv(all_rows, filename):
    """Save projection data to CSV"""
    if not all_rows:
        print("No data to save")
        return
    
    df = pd.DataFrame(all_rows)
    
    # Ensure output directory exists
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    filepath = output_dir / filename
    df.to_csv(filepath, index=False)
    
    print(f"✅ Saved {len(all_rows)} projection records to {filepath}")
    print(f"📊 Data includes {df['school_id'].nunique()} schools")
    print(f"📅 Projection years: {', '.join(sorted(df['projection_year'].unique()))}")

def main():
    global proxy_process
    
    try:
        print("🚀 Starting enrollment projections CSV generation...")
        
        # Start Cloud SQL Proxy
        proxy_process, port = start_cloud_sql_proxy()
        
        # Connect to database
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        print("📊 Fetching schools with enrollment data...")
        schools = fetch_schools_sample(engine, limit=100)  # Start with 100 schools
        print(f"Found {len(schools)} schools to process")
        
        all_projections = []
        processed = 0
        errors = 0
        
        for school in schools:
            try:
                print(f"Processing school {processed + 1}/{len(schools)}: {school['school_name']} ({school['ncessch']})")
                
                # Fetch enrollment data
                enrollment_data = fetch_enrollment_data(engine, school['id'])
                
                if not enrollment_data:
                    print(f"  ⚠️  No enrollment data found")
                    continue
                
                # Prepare school data structure
                school_data = {
                    'id': school['id'],
                    'ncessch': school['ncessch'],
                    'school_name': school['school_name'],
                    'enrollment': enrollment_data
                }
                
                # Generate projections
                projected_school = generate_school_projections(school_data)
                
                if projected_school and 'projections' in projected_school:
                    # Format for CSV
                    rows = format_projections_for_csv(projected_school)
                    all_projections.extend(rows)
                    print(f"  ✅ Generated {len(rows)} projection records")
                else:
                    print(f"  ❌ Failed to generate projections")
                    errors += 1
                
                processed += 1
                
            except Exception as e:
                print(f"  ❌ Error processing school: {str(e)}")
                errors += 1
                continue
        
        # Save results
        if all_projections:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"school_projections_{timestamp}.csv"
            save_to_csv(all_projections, filename)
            
            print(f"\n📋 Summary:")
            print(f"  Processed: {processed} schools")
            print(f"  Errors: {errors} schools")
            print(f"  Total projections: {len(all_projections)} records")
        else:
            print("\n❌ No projections were generated")
        
        return 0
        
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
    print("="*60)
    print("ENROLLMENT PROJECTIONS CSV GENERATOR")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result)