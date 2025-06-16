#!/usr/bin/env python3
"""
Projections Analysis Script
Identifies schools that need enrollment projections and analyzes data quality for population generation
"""

import os
import sys
import subprocess
import time
import socket
import signal
import argparse
from pathlib import Path
import logging
from typing import Dict, List, Tuple
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

# Target data years for projections
PUBLIC_DATA_YEAR = 2023   # Public schools
PRIVATE_DATA_YEAR = 2021  # Private schools

# Minimum years of data needed for reliable projections
MIN_YEARS_FOR_PROJECTIONS = 3
OPTIMAL_YEARS_FOR_PROJECTIONS = 5

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
                '-max_connections=50',
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

def test_connection(port):
    """Test database connection"""
    try:
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        print(f"Testing connection: postgresql://{DB_USER}:***@localhost:{port}/{DB_NAME}")
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print("✅ Database connection successful!")
            return True, engine
            
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return False, None

def analyze_projection_populations(engine):
    """Analyze school populations that need projections"""
    print("\n" + "="*80)
    print("📊 PROJECTION POPULATION ANALYSIS")
    print("="*80)
    
    with engine.connect() as conn:
        # Get overview of schools by data year and ownership
        result = conn.execute(text("""
            WITH school_data_years AS (
                SELECT DISTINCT 
                    s.id as school_id,
                    sd.data_year,
                    sd.school_ownership,
                    sd.school_operational_model,
                    sd.system_name
                FROM schools s
                INNER JOIN school_directory_data sd ON s.id = sd.school_id
                WHERE sd.is_current = true
            ),
            enrollment_years AS (
                SELECT DISTINCT
                    school_id,
                    data_year,
                    COUNT(DISTINCT school_year) as year_count,
                    COUNT(DISTINCT grade) as grade_count,
                    MIN(school_year) as earliest_year,
                    MAX(school_year) as latest_year
                FROM school_enrollments
                GROUP BY school_id, data_year
            )
            SELECT 
                sdy.data_year,
                sdy.school_ownership,
                COUNT(DISTINCT sdy.school_id) as total_schools,
                COUNT(DISTINCT CASE WHEN ey.school_id IS NOT NULL THEN sdy.school_id END) as schools_with_enrollment,
                COUNT(DISTINCT CASE WHEN ey.year_count >= :min_years THEN sdy.school_id END) as schools_projection_ready,
                COUNT(DISTINCT CASE WHEN ey.year_count >= :optimal_years THEN sdy.school_id END) as schools_optimal_data,
                ROUND(AVG(ey.year_count), 1) as avg_years_of_data,
                ROUND(AVG(ey.grade_count), 1) as avg_grades_per_year
            FROM school_data_years sdy
            LEFT JOIN enrollment_years ey ON sdy.school_id = ey.school_id AND sdy.data_year = ey.data_year
            WHERE sdy.data_year IN (:public_year, :private_year)
            GROUP BY sdy.data_year, sdy.school_ownership
            ORDER BY sdy.data_year, sdy.school_ownership
        """), {
            'public_year': PUBLIC_DATA_YEAR,
            'private_year': PRIVATE_DATA_YEAR,
            'min_years': MIN_YEARS_FOR_PROJECTIONS,
            'optimal_years': OPTIMAL_YEARS_FOR_PROJECTIONS
        })
        
        stats = result.fetchall()
        
        print("📋 POPULATION OVERVIEW:")
        total_ready = 0
        total_schools = 0
        
        for stat in stats:
            data_year = stat.data_year
            ownership = stat.school_ownership or 'Unknown'
            year_label = f"{data_year} ({ownership})"
            
            print(f"\n🏫 {year_label}:")
            print(f"  📊 Total schools: {stat.total_schools:,}")
            print(f"  📈 With enrollment data: {stat.schools_with_enrollment:,} ({100.0 * stat.schools_with_enrollment / stat.total_schools:.1f}%)")
            print(f"  ✅ Ready for projections (≥{MIN_YEARS_FOR_PROJECTIONS} years): {stat.schools_projection_ready:,} ({100.0 * stat.schools_projection_ready / stat.total_schools:.1f}%)")
            print(f"  🎯 Optimal data (≥{OPTIMAL_YEARS_FOR_PROJECTIONS} years): {stat.schools_optimal_data:,} ({100.0 * stat.schools_optimal_data / stat.total_schools:.1f}%)")
            if stat.avg_years_of_data:
                print(f"  📅 Average years of data: {stat.avg_years_of_data}")
                print(f"  🎓 Average grades per year: {stat.avg_grades_per_year}")
            
            total_ready += stat.schools_projection_ready or 0
            total_schools += stat.total_schools
        
        print(f"\n🎯 TOTAL PROJECTION-READY SCHOOLS: {total_ready:,} out of {total_schools:,} ({100.0 * total_ready / total_schools:.1f}%)")
        
        return stats

def analyze_data_quality_issues(engine):
    """Analyze data quality issues that might affect projections"""
    print("\n" + "-"*80)
    print("🔍 DATA QUALITY ANALYSIS")
    print("-"*80)
    
    with engine.connect() as conn:
        # Check for common data quality issues
        result = conn.execute(text("""
            WITH school_populations AS (
                SELECT DISTINCT 
                    s.id as school_id,
                    sd.data_year,
                    sd.school_ownership,
                    sd.system_name
                FROM schools s
                INNER JOIN school_directory_data sd ON s.id = sd.school_id
                WHERE sd.is_current = true
                  AND sd.data_year IN (:public_year, :private_year)
            ),
            enrollment_issues AS (
                SELECT 
                    sp.school_id,
                    sp.data_year,
                    sp.school_ownership,
                    sp.system_name,
                    COUNT(DISTINCT se.school_year) as years_of_data,
                    COUNT(DISTINCT se.grade) as total_grades,
                    COUNT(DISTINCT CASE WHEN se.total_enrollment = 0 THEN se.grade END) as zero_enrollment_grades,
                    COUNT(DISTINCT CASE WHEN se.total_enrollment < 0 THEN se.grade END) as negative_enrollment_grades,
                    COUNT(DISTINCT CASE WHEN se.total_enrollment > 1000 THEN se.grade END) as high_enrollment_grades,
                    MAX(se.total_enrollment) as max_enrollment,
                    MIN(se.total_enrollment) as min_enrollment,
                    STRING_AGG(DISTINCT se.grade ORDER BY se.grade) as grades_offered
                FROM school_populations sp
                LEFT JOIN school_enrollments se ON sp.school_id = se.school_id AND sp.data_year = se.data_year
                GROUP BY sp.school_id, sp.data_year, sp.school_ownership, sp.system_name
            )
            SELECT 
                data_year,
                school_ownership,
                COUNT(*) as total_schools,
                COUNT(CASE WHEN years_of_data = 0 THEN 1 END) as no_enrollment_data,
                COUNT(CASE WHEN years_of_data BETWEEN 1 AND 2 THEN 1 END) as insufficient_data,
                COUNT(CASE WHEN zero_enrollment_grades > 0 THEN 1 END) as has_zero_enrollments,
                COUNT(CASE WHEN negative_enrollment_grades > 0 THEN 1 END) as has_negative_enrollments,
                COUNT(CASE WHEN high_enrollment_grades > 0 THEN 1 END) as has_high_enrollments,
                COUNT(CASE WHEN years_of_data >= :min_years THEN 1 END) as projection_ready
            FROM enrollment_issues
            GROUP BY data_year, school_ownership
            ORDER BY data_year, school_ownership
        """), {
            'public_year': PUBLIC_DATA_YEAR,
            'private_year': PRIVATE_DATA_YEAR,
            'min_years': MIN_YEARS_FOR_PROJECTIONS
        })
        
        quality_issues = result.fetchall()
        
        print("⚠️ DATA QUALITY ISSUES:")
        
        for issue in quality_issues:
            data_year = issue.data_year
            ownership = issue.school_ownership or 'Unknown'
            year_label = f"{data_year} ({ownership})"
            
            print(f"\n📊 {year_label} - {issue.total_schools:,} schools:")
            if issue.no_enrollment_data > 0:
                print(f"  ❌ No enrollment data: {issue.no_enrollment_data:,} ({100.0 * issue.no_enrollment_data / issue.total_schools:.1f}%)")
            if issue.insufficient_data > 0:
                print(f"  ⚠️  Insufficient data (1-2 years): {issue.insufficient_data:,} ({100.0 * issue.insufficient_data / issue.total_schools:.1f}%)")
            if issue.has_zero_enrollments > 0:
                print(f"  🔴 Has zero enrollments: {issue.has_zero_enrollments:,} ({100.0 * issue.has_zero_enrollments / issue.total_schools:.1f}%)")
            if issue.has_negative_enrollments > 0:
                print(f"  ❗ Has negative enrollments: {issue.has_negative_enrollments:,} ({100.0 * issue.has_negative_enrollments / issue.total_schools:.1f}%)")
            if issue.has_high_enrollments > 0:
                print(f"  📈 Has very high enrollments (>1000): {issue.has_high_enrollments:,} ({100.0 * issue.has_high_enrollments / issue.total_schools:.1f}%)")
            
            print(f"  ✅ Ready for projections: {issue.projection_ready:,} ({100.0 * issue.projection_ready / issue.total_schools:.1f}%)")
        
        return quality_issues

def identify_projection_ready_schools(engine):
    """Identify schools ready for projection processing"""
    print("\n" + "-"*80)
    print("🎯 PROJECTION-READY SCHOOLS")
    print("-"*80)
    
    ready_schools = {'public': [], 'private': []}
    
    with engine.connect() as conn:
        # Get schools ready for projections by data year
        for data_year, school_type in [(PUBLIC_DATA_YEAR, 'public'), (PRIVATE_DATA_YEAR, 'private')]:
            result = conn.execute(text("""
                WITH school_populations AS (
                    SELECT DISTINCT 
                        s.id as school_id,
                        sd.system_name,
                        sd.school_ownership,
                        sd.ncessch
                    FROM schools s
                    INNER JOIN school_directory_data sd ON s.id = sd.school_id
                    WHERE sd.is_current = true
                      AND sd.data_year = :data_year
                ),
                enrollment_summary AS (
                    SELECT 
                        sp.school_id,
                        sp.system_name,
                        sp.school_ownership,
                        sp.ncessch,
                        COUNT(DISTINCT se.school_year) as years_of_data,
                        COUNT(DISTINCT se.grade) as total_grades,
                        MIN(se.school_year) as earliest_year,
                        MAX(se.school_year) as latest_year,
                        SUM(se.total_enrollment) as total_enrollment,
                        STRING_AGG(DISTINCT se.grade ORDER BY se.grade) as grades_offered
                    FROM school_populations sp
                    INNER JOIN school_enrollments se ON sp.school_id = se.school_id AND se.data_year = :data_year
                    WHERE se.total_enrollment >= 0  -- Exclude negative enrollments
                    GROUP BY sp.school_id, sp.system_name, sp.school_ownership, sp.ncessch
                    HAVING COUNT(DISTINCT se.school_year) >= :min_years
                       AND SUM(se.total_enrollment) > 0  -- Must have some enrollment
                )
                SELECT 
                    school_id,
                    system_name,
                    school_ownership,
                    ncessch,
                    years_of_data,
                    total_grades,
                    earliest_year,
                    latest_year,
                    total_enrollment,
                    grades_offered
                FROM enrollment_summary
                ORDER BY years_of_data DESC, total_enrollment DESC
            """), {
                'data_year': data_year,
                'min_years': MIN_YEARS_FOR_PROJECTIONS
            })
            
            schools = result.fetchall()
            ready_schools[school_type] = schools
            
            print(f"\n📊 {data_year} ({school_type.upper()}) - {len(schools):,} schools ready:")
            
            if schools:
                # Summary stats
                years_stats = [s.years_of_data for s in schools]
                enrollment_stats = [s.total_enrollment for s in schools]
                
                print(f"  📅 Years of data: {min(years_stats)}-{max(years_stats)} (avg: {sum(years_stats)/len(years_stats):.1f})")
                print(f"  👥 Total enrollment: {min(enrollment_stats):,}-{max(enrollment_stats):,} (avg: {sum(enrollment_stats)//len(enrollment_stats):,})")
                
                # Top schools by data completeness
                print(f"\n  🏆 Top schools by data completeness:")
                for i, school in enumerate(schools[:10]):
                    ownership = school.school_ownership or 'Unknown'
                    print(f"    {i+1}. {school.system_name} ({ownership})")
                    print(f"       NCES: {school.ncessch} | Years: {school.years_of_data} | Grades: {school.total_grades} | Enrollment: {school.total_enrollment:,}")
                
                if len(schools) > 10:
                    print(f"    ... and {len(schools) - 10:,} more schools")
            else:
                print("  ❌ No schools meet the minimum requirements")
    
    return ready_schools

def generate_processing_recommendations(population_stats, quality_issues, ready_schools):
    """Generate recommendations for projection processing"""
    print("\n" + "="*80)
    print("📋 PROCESSING RECOMMENDATIONS")
    print("="*80)
    
    total_ready = sum(len(schools) for schools in ready_schools.values())
    
    print(f"🎯 PROJECTION PROCESSING PLAN:")
    print(f"  📊 Total schools ready for processing: {total_ready:,}")
    
    for school_type, schools in ready_schools.items():
        if schools:
            data_year = PUBLIC_DATA_YEAR if school_type == 'public' else PRIVATE_DATA_YEAR
            print(f"\n📈 {school_type.upper()} SCHOOLS ({data_year}):")
            print(f"  🏫 Count: {len(schools):,} schools")
            print(f"  ⏱️  Estimated processing time: {len(schools) * 2:.1f} seconds ({len(schools) * 2 / 60:.1f} minutes)")
            
            # Data quality assessment
            high_quality = sum(1 for s in schools if s.years_of_data >= OPTIMAL_YEARS_FOR_PROJECTIONS)
            print(f"  🎖️  High quality data (≥{OPTIMAL_YEARS_FOR_PROJECTIONS} years): {high_quality:,} schools ({100.0 * high_quality / len(schools):.1f}%)")
            
            # Grade coverage
            grade_counts = [s.total_grades for s in schools]
            print(f"  🎓 Grade coverage: {min(grade_counts)}-{max(grade_counts)} grades (avg: {sum(grade_counts)/len(grade_counts):.1f})")
            
            print(f"  💾 Output: populations_{school_type}_{data_year}.csv")
    
    print(f"\n🚀 NEXT STEPS:")
    print(f"  1. Run 02_projections_processing.py to generate populations")
    print(f"  2. Review output CSV files for data quality")
    print(f"  3. Load projections into school_projections table")
    print(f"  4. Validate projection results")
    
    # Warnings
    warnings = []
    for issue_set in quality_issues:
        if issue_set.has_negative_enrollments > 0:
            warnings.append(f"⚠️ {issue_set.has_negative_enrollments} schools have negative enrollments")
        if issue_set.no_enrollment_data > issue_set.total_schools * 0.1:  # More than 10% missing data
            warnings.append(f"⚠️ {issue_set.no_enrollment_data} schools have no enrollment data")
    
    if warnings:
        print(f"\n⚠️ WARNINGS:")
        for warning in warnings:
            print(f"  {warning}")
    
    return {
        'total_ready': total_ready,
        'public_count': len(ready_schools['public']),
        'private_count': len(ready_schools['private']),
        'estimated_time_minutes': total_ready * 2 / 60,
        'warnings': warnings
    }

def export_analysis_results(ready_schools, output_dir="01_projections"):
    """Export analysis results to CSV files"""
    print("\n" + "-"*80)
    print("💾 EXPORTING ANALYSIS RESULTS")
    print("-"*80)
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for school_type, schools in ready_schools.items():
        if schools:
            # Convert to DataFrame
            df = pd.DataFrame([
                {
                    'school_id': s.school_id,
                    'system_name': s.system_name,
                    'school_ownership': s.school_ownership,
                    'ncessch': s.ncessch,
                    'years_of_data': s.years_of_data,
                    'total_grades': s.total_grades,
                    'earliest_year': s.earliest_year,
                    'latest_year': s.latest_year,
                    'total_enrollment': s.total_enrollment,
                    'grades_offered': s.grades_offered,
                    'data_quality': 'high' if s.years_of_data >= OPTIMAL_YEARS_FOR_PROJECTIONS else 'sufficient',
                    'analysis_date': datetime.now().isoformat()
                }
                for s in schools
            ])
            
            # Export to CSV
            filename = f"projection_ready_{school_type}_{timestamp}.csv"
            filepath = output_path / filename
            df.to_csv(filepath, index=False)
            
            print(f"✅ Exported {len(schools):,} {school_type} schools to {filepath}")
    
    print(f"\n📁 Analysis results saved to: {output_path.absolute()}")

def main():
    global proxy_process
    
    try:
        print("🚀 Starting enrollment projections analysis...")
        
        # Start Cloud SQL Proxy
        proxy_process, port = start_cloud_sql_proxy()
        
        # Connect to database
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{port}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        # Run analysis pipeline
        print("📊 Analyzing projection populations...")
        population_stats = analyze_projection_populations(engine)
        
        print("🔍 Analyzing data quality...")
        quality_issues = analyze_data_quality_issues(engine)
        
        print("🎯 Identifying projection-ready schools...")
        ready_schools = identify_projection_ready_schools(engine)
        
        print("📋 Generating recommendations...")
        recommendations = generate_processing_recommendations(population_stats, quality_issues, ready_schools)
        
        print("💾 Exporting results...")
        export_analysis_results(ready_schools)
        
        print(f"\n" + "="*80)
        print("✅ ANALYSIS COMPLETE")
        print("="*80)
        print(f"📊 Ready for processing: {recommendations['total_ready']:,} schools")
        print(f"   🏫 Public ({PUBLIC_DATA_YEAR}): {recommendations['public_count']:,}")
        print(f"   🏫 Private ({PRIVATE_DATA_YEAR}): {recommendations['private_count']:,}")
        print(f"⏱️  Estimated processing time: {recommendations['estimated_time_minutes']:.1f} minutes")
        
        if recommendations['warnings']:
            print(f"\n⚠️ Warnings: {len(recommendations['warnings'])}")
            
        print(f"\n🚀 Next: Run 02_projections_processing.py to generate populations")
        
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
    print("="*80)
    print("ENROLLMENT PROJECTIONS ANALYSIS")
    print("="*80)
    result = main()
    print("="*80)
    sys.exit(result) 