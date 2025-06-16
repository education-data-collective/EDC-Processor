#!/usr/bin/env python3
"""Test script to show schools by specific drive times"""

from sqlalchemy import create_engine, text
import subprocess
import time
import socket

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def main():
    port = find_free_port()
    proxy_process = subprocess.Popen([
        'cloud-sql-proxy',
        f'-instances=enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db=tcp:{port}',
        f'-credential_file=../etl-service-account-key.json',
        '-max_connections=10',
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(3)

    try:
        engine = create_engine(f'postgresql://admin:edc4thew!n@localhost:{port}/edc_unified')

        with engine.connect() as conn:
            print("🔍 Schools by drive time for location 1691832:")
            
            # Check schools by drive time for location 1691832
            for drive_time in [5, 10, 15]:
                result = conn.execute(text('''
                    SELECT COUNT(*) as school_count
                    FROM nearby_school_polygons nsp
                    JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                    WHERE spr.location_id = :location_id
                      AND spr.drive_time = :drive_time
                      AND spr.data_year = :data_year
                '''), {'location_id': 1691832, 'drive_time': drive_time, 'data_year': 2023}).fetchone()
                print(f'  {drive_time}-minute drive: {result[0]} schools')
            
            print("\n🎯 Schools ONLY in specific drive time zones:")
            
            # Schools only in 5-minute zone
            result_5_only = conn.execute(text('''
                SELECT COUNT(DISTINCT nsp.school_uuid) as school_count
                FROM nearby_school_polygons nsp
                JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.location_id = :location_id
                  AND spr.drive_time = 5
                  AND spr.data_year = :data_year
            '''), {'location_id': 1691832, 'data_year': 2023}).fetchone()
            
            # Schools in 10-minute but NOT in 5-minute
            result_10_only = conn.execute(text('''
                SELECT COUNT(DISTINCT nsp.school_uuid) as school_count
                FROM nearby_school_polygons nsp
                JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.location_id = :location_id
                  AND spr.drive_time = 10
                  AND spr.data_year = :data_year
                  AND nsp.school_uuid NOT IN (
                    SELECT nsp2.school_uuid 
                    FROM nearby_school_polygons nsp2
                    JOIN school_polygon_relationships spr2 ON nsp2.polygon_relationship_id = spr2.id
                    WHERE spr2.location_id = :location_id 
                      AND spr2.drive_time = 5
                      AND spr2.data_year = :data_year
                  )
            '''), {'location_id': 1691832, 'data_year': 2023}).fetchone()
            
            # Schools in 15-minute but NOT in 10-minute
            result_15_only = conn.execute(text('''
                SELECT COUNT(DISTINCT nsp.school_uuid) as school_count
                FROM nearby_school_polygons nsp
                JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.location_id = :location_id
                  AND spr.drive_time = 15
                  AND spr.data_year = :data_year
                  AND nsp.school_uuid NOT IN (
                    SELECT nsp2.school_uuid 
                    FROM nearby_school_polygons nsp2
                    JOIN school_polygon_relationships spr2 ON nsp2.polygon_relationship_id = spr2.id
                    WHERE spr2.location_id = :location_id 
                      AND spr2.drive_time = 10
                      AND spr2.data_year = :data_year
                  )
            '''), {'location_id': 1691832, 'data_year': 2023}).fetchone()
            
            print(f'  Only in 5-minute zone: {result_5_only[0]} schools')
            print(f'  Only in 10-minute zone (not 5): {result_10_only[0]} schools') 
            print(f'  Only in 15-minute zone (not 10): {result_15_only[0]} schools')

    finally:
        proxy_process.terminate()

if __name__ == "__main__":
    main() 