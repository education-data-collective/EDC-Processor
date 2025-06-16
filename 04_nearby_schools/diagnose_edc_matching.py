#!/usr/bin/env python3
"""Diagnostic script to investigate EDC school matching issues"""

import csv
import subprocess
import time
import socket
from sqlalchemy import create_engine, text

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def load_edc_schools():
    """Load EDC schools from CSV"""
    edc_schools = set()
    with open('../edc_schools/firebase_data/edc_schools.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            school_id = row['school_id'].strip()
            if school_id:
                edc_schools.add(school_id)
    return edc_schools

def main():
    port = find_free_port()
    
    # Try different proxy command names
    proxy_commands = ['cloud-sql-proxy', 'cloud_sql_proxy']
    proxy_cmd = None
    
    for cmd in proxy_commands:
        try:
            subprocess.run([cmd, '--version'], capture_output=True, check=True)
            proxy_cmd = [
                cmd,
                f'-instances=enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db=tcp:{port}',
                f'-credential_file=../etl-service-account-key.json',
                '-max_connections=10',
            ]
            break
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    if not proxy_cmd:
        raise Exception("Cloud SQL Proxy not found. Please install it first.")
    
    proxy_process = subprocess.Popen(proxy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(3)

    try:
        engine = create_engine(f'postgresql://admin:edc4thew!n@localhost:{port}/edc_unified')
        edc_schools = load_edc_schools()
        
        print(f"📊 EDC Schools from CSV: {len(edc_schools)}")
        
        with engine.connect() as conn:
            # 1. Check how many EDC schools match in the database
            matched_schools = conn.execute(text("""
                SELECT COUNT(DISTINCT s.uuid) as matched_count
                FROM schools s
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                WHERE sd.ncessch = ANY(:school_ids) 
                   OR CONCAT(sd.ncessch, '-', sd.split_suffix) = ANY(:school_ids)
                   OR sd.state_school_id = ANY(:school_ids)
            """), {'school_ids': list(edc_schools)}).fetchone()
            
            print(f"🎯 EDC Schools matched in database: {matched_schools[0]}")
            
            # 2. Check how many have location data for 2023
            with_locations_2023 = conn.execute(text("""
                SELECT COUNT(DISTINCT s.uuid) as with_locations
                FROM schools s
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                JOIN school_locations sl ON s.id = sl.school_id
                JOIN location_points lp ON sl.location_id = lp.id
                WHERE (sd.ncessch = ANY(:school_ids) 
                       OR CONCAT(sd.ncessch, '-', sd.split_suffix) = ANY(:school_ids)
                       OR sd.state_school_id = ANY(:school_ids))
                  AND sl.data_year = 2023
                  AND sl.is_current = true
                  AND lp.latitude IS NOT NULL 
                  AND lp.longitude IS NOT NULL
            """), {'school_ids': list(edc_schools)}).fetchone()
            
            print(f"📍 EDC Schools with 2023 location data: {with_locations_2023[0]}")
            
            # 3. Check how many have ESRI data
            with_esri = conn.execute(text("""
                SELECT COUNT(DISTINCT s.uuid) as with_esri
                FROM schools s
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                JOIN school_locations sl ON s.id = sl.school_id
                JOIN location_points lp ON sl.location_id = lp.id
                JOIN (
                    SELECT DISTINCT location_id 
                    FROM esri_demographic_data
                    WHERE drive_time_polygon IS NOT NULL
                ) esri ON lp.id = esri.location_id
                WHERE (sd.ncessch = ANY(:school_ids) 
                       OR CONCAT(sd.ncessch, '-', sd.split_suffix) = ANY(:school_ids)
                       OR sd.state_school_id = ANY(:school_ids))
                  AND sl.data_year = 2023
                  AND sl.is_current = true
                  AND lp.latitude IS NOT NULL 
                  AND lp.longitude IS NOT NULL
            """), {'school_ids': list(edc_schools)}).fetchone()
            
            print(f"🗺️  EDC Schools with ESRI polygon data: {with_esri[0]}")
            
            # 4. Check unique locations for all schools (not just EDC)
            all_locations_2023 = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT lp.id) as total_locations,
                    COUNT(DISTINCT CONCAT(lp.latitude::text, ',', lp.longitude::text)) as unique_coordinates
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                WHERE sl.data_year = 2023
                  AND sl.is_current = true
                  AND lp.latitude IS NOT NULL 
                  AND lp.longitude IS NOT NULL
            """)).fetchone()
            
            print(f"\n📊 All Schools 2023 Data:")
            print(f"  Total location records: {all_locations_2023[0]}")
            print(f"  Unique coordinates: {all_locations_2023[1]}")
            
            # 5. Check locations with ESRI data
            esri_locations = conn.execute(text("""
                SELECT COUNT(DISTINCT lp.id) as locations_with_esri
                FROM location_points lp
                JOIN school_locations sl ON lp.id = sl.location_id
                JOIN (
                    SELECT DISTINCT location_id 
                    FROM esri_demographic_data
                    WHERE drive_time_polygon IS NOT NULL
                ) esri ON lp.id = esri.location_id
                WHERE sl.data_year = 2023
                  AND sl.is_current = true
                  AND lp.latitude IS NOT NULL 
                  AND lp.longitude IS NOT NULL
            """)).fetchone()
            
            print(f"  Locations with ESRI data: {esri_locations[0]}")
            
            # 6. Sample of unmatched EDC school IDs
            print(f"\n🔍 Sample EDC School IDs (first 10):")
            sample_ids = list(edc_schools)[:10]
            for school_id in sample_ids:
                print(f"  {school_id}")
            
            # 7. Sample of database school IDs
            print(f"\n🔍 Sample Database School IDs (first 10):")
            db_sample = conn.execute(text("""
                SELECT COALESCE(CONCAT(sd.ncessch, '-', sd.split_suffix), sd.ncessch, sd.state_school_id) as school_id
                FROM school_directory sd
                WHERE sd.is_current = true
                LIMIT 10
            """)).fetchall()
            
            for row in db_sample:
                print(f"  {row[0]}")

    finally:
        proxy_process.terminate()

if __name__ == "__main__":
    main() 