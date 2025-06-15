#!/usr/bin/env python3
"""
Geocoding Analysis Script
Identifies location_points that need geocoding (missing coordinates and/or county data)
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

def analyze_geocoding_needs(engine):
    """Analyze which location_points need geocoding"""
    print("\n" + "="*80)
    print("🌐 GEOCODING NEEDS ANALYSIS")
    print("="*80)
    
    with engine.connect() as conn:
        # Get overall statistics for location_points used by schools
        result = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT lp.id) as total_active_locations,
                COUNT(DISTINCT CASE WHEN lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL THEN lp.id END) as has_coordinates,
                COUNT(DISTINCT CASE WHEN lp.county IS NOT NULL AND lp.county != '' THEN lp.id END) as has_county,
                COUNT(DISTINCT CASE WHEN (lp.latitude IS NULL OR lp.longitude IS NULL) THEN lp.id END) as missing_coordinates,
                COUNT(DISTINCT CASE WHEN (lp.county IS NULL OR lp.county = '') THEN lp.id END) as missing_county,
                COUNT(DISTINCT CASE WHEN (lp.latitude IS NULL OR lp.longitude IS NULL) 
                                     AND (lp.county IS NULL OR lp.county = '') THEN lp.id END) as missing_both,
                COUNT(DISTINCT CASE WHEN (lp.address IS NOT NULL AND lp.address != '') 
                                      OR (lp.city IS NOT NULL AND lp.city != '') THEN lp.id END) as has_address_data
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
        """))
        
        stats = result.fetchone()
        
        print("📊 OVERALL STATISTICS (Active Locations Only):")
        print(f"  📍 Total active locations: {stats.total_active_locations:,}")
        print(f"  ✅ Has coordinates: {stats.has_coordinates:,} ({100.0 * stats.has_coordinates / stats.total_active_locations:.1f}%)")
        print(f"  🏛️  Has county: {stats.has_county:,} ({100.0 * stats.has_county / stats.total_active_locations:.1f}%)")
        print(f"  ❌ Missing coordinates: {stats.missing_coordinates:,} ({100.0 * stats.missing_coordinates / stats.total_active_locations:.1f}%)")
        print(f"  ❌ Missing county: {stats.missing_county:,} ({100.0 * stats.missing_county / stats.total_active_locations:.1f}%)")
        print(f"  ❌ Missing both: {stats.missing_both:,} ({100.0 * stats.missing_both / stats.total_active_locations:.1f}%)")
        print(f"  📍 Has address data: {stats.has_address_data:,} ({100.0 * stats.has_address_data / stats.total_active_locations:.1f}%)")
        
        return stats

def identify_coordinates_needed(engine):
    """Identify locations that need coordinate geocoding"""
    print("\n" + "-"*80)
    print("🗺️  LOCATIONS NEEDING COORDINATE GEOCODING")
    print("-"*80)
    
    with engine.connect() as conn:
        # Find locations missing coordinates but with address data
        result = conn.execute(text("""
            SELECT 
                lp.id as location_point_id,
                lp.address,
                lp.city,
                lp.state,
                lp.zip_code,
                lp.county,
                COUNT(sl.school_id) as school_count,
                array_agg(DISTINCT sl.school_id ORDER BY sl.school_id) as school_ids
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
            WHERE (lp.latitude IS NULL OR lp.longitude IS NULL)
              AND ((lp.address IS NOT NULL AND lp.address != '') 
                   OR (lp.city IS NOT NULL AND lp.city != '')
                   OR (lp.zip_code IS NOT NULL AND lp.zip_code != ''))
            GROUP BY lp.id, lp.address, lp.city, lp.state, lp.zip_code, lp.county
            ORDER BY school_count DESC, lp.id
        """))
        
        coordinates_needed = result.fetchall()
        
        if coordinates_needed:
            print(f"Found {len(coordinates_needed):,} locations needing coordinate geocoding")
            print("\nTop 10 locations by number of schools affected:")
            
            for i, record in enumerate(coordinates_needed[:10]):
                address_parts = []
                if record.address: address_parts.append(record.address)
                if record.city: address_parts.append(record.city)
                if record.state: address_parts.append(record.state)
                if record.zip_code: address_parts.append(record.zip_code)
                
                address = ', '.join(address_parts) if address_parts else 'No address data'
                county_info = f" | County: {record.county}" if record.county else " | No county"
                
                print(f"  {i+1}. ID: {record.location_point_id} | Schools: {record.school_count}")
                print(f"      Address: {address}{county_info}")
                print()
        else:
            print("✅ All active locations have coordinates!")
            
        return coordinates_needed

def identify_county_needed(engine):
    """Identify locations that need county geocoding"""
    print("\n" + "-"*80)
    print("🏛️  LOCATIONS NEEDING COUNTY GEOCODING")
    print("-"*80)
    
    with engine.connect() as conn:
        # Find locations missing county data
        result = conn.execute(text("""
            SELECT 
                lp.id as location_point_id,
                lp.address,
                lp.city,
                lp.state,
                lp.zip_code,
                lp.latitude,
                lp.longitude,
                COUNT(sl.school_id) as school_count,
                array_agg(DISTINCT sl.school_id ORDER BY sl.school_id) as school_ids
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
            WHERE (lp.county IS NULL OR lp.county = '')
              AND ((lp.address IS NOT NULL AND lp.address != '') 
                   OR (lp.city IS NOT NULL AND lp.city != '')
                   OR (lp.zip_code IS NOT NULL AND lp.zip_code != '')
                   OR (lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL))
            GROUP BY lp.id, lp.address, lp.city, lp.state, lp.zip_code, lp.latitude, lp.longitude
            ORDER BY school_count DESC, lp.id
        """))
        
        county_needed = result.fetchall()
        
        if county_needed:
            print(f"Found {len(county_needed):,} locations needing county geocoding")
            print("\nTop 10 locations by number of schools affected:")
            
            for i, record in enumerate(county_needed[:10]):
                # Build address string
                address_parts = []
                if record.address: address_parts.append(record.address)
                if record.city: address_parts.append(record.city)
                if record.state: address_parts.append(record.state)
                if record.zip_code: address_parts.append(record.zip_code)
                
                address = ', '.join(address_parts) if address_parts else 'No address data'
                
                # Show coordinates if available
                coords = ""
                if record.latitude and record.longitude:
                    coords = f" | Coords: ({record.latitude:.6f}, {record.longitude:.6f})"
                else:
                    coords = " | No coordinates"
                
                print(f"  {i+1}. ID: {record.location_point_id} | Schools: {record.school_count}")
                print(f"      Address: {address}{coords}")
                print()
        else:
            print("✅ All active locations have county data!")
            
        return county_needed

def identify_no_location_data(engine):
    """Identify locations that have no location data at all"""
    print("\n" + "-"*80)
    print("❌ LOCATIONS WITH NO GEOCODABLE DATA")
    print("-"*80)
    
    with engine.connect() as conn:
        # Find locations with no usable location data
        result = conn.execute(text("""
            SELECT 
                lp.id as location_point_id,
                COUNT(sl.school_id) as school_count,
                array_agg(DISTINCT sl.school_id ORDER BY sl.school_id) as school_ids
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
            WHERE (lp.latitude IS NULL OR lp.longitude IS NULL)
              AND (lp.county IS NULL OR lp.county = '')
              AND (lp.address IS NULL OR lp.address = '')
              AND (lp.city IS NULL OR lp.city = '')
              AND (lp.zip_code IS NULL OR lp.zip_code = '')
            GROUP BY lp.id
            ORDER BY school_count DESC
        """))
        
        no_data = result.fetchall()
        
        if no_data:
            print(f"⚠️  Found {len(no_data):,} locations with NO geocodable data")
            print("These locations cannot be geocoded and may need manual intervention:")
            
            for i, record in enumerate(no_data[:10]):
                print(f"  {i+1}. Location ID: {record.location_point_id} | Used by {record.school_count} school(s)")
                
            if len(no_data) > 10:
                print(f"  ... and {len(no_data) - 10:,} more")
        else:
            print("✅ All active locations have some geocodable data!")
            
        return no_data

def export_geocoding_lists(coordinates_needed, county_needed, no_data, output_dir="01_geocode"):
    """Export lists of locations needing geocoding to CSV files"""
    print("\n" + "-"*80)
    print("📊 EXPORTING GEOCODING LISTS")
    print("-"*80)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Export coordinates needed
    if coordinates_needed:
        coords_df = pd.DataFrame([
            {
                'location_point_id': r.location_point_id,
                'address': r.address,
                'city': r.city,
                'state': r.state,
                'zip_code': r.zip_code,
                'county': r.county,
                'school_count': r.school_count,
                'school_ids': ','.join(map(str, r.school_ids)) if r.school_ids else ''
            } for r in coordinates_needed
        ])
        coords_file = f"{output_dir}/locations_need_coordinates.csv"
        coords_df.to_csv(coords_file, index=False)
        print(f"  ✅ Exported {len(coordinates_needed):,} locations needing coordinates to {coords_file}")
    
    # Export county needed
    if county_needed:
        county_df = pd.DataFrame([
            {
                'location_point_id': r.location_point_id,
                'address': r.address,
                'city': r.city,
                'state': r.state,
                'zip_code': r.zip_code,
                'latitude': r.latitude,
                'longitude': r.longitude,
                'school_count': r.school_count,
                'school_ids': ','.join(map(str, r.school_ids)) if r.school_ids else ''
            } for r in county_needed
        ])
        county_file = f"{output_dir}/locations_need_county.csv"
        county_df.to_csv(county_file, index=False)
        print(f"  ✅ Exported {len(county_needed):,} locations needing county to {county_file}")
    
    # Export no data locations
    if no_data:
        no_data_df = pd.DataFrame([
            {
                'location_point_id': r.location_point_id,
                'school_count': r.school_count,
                'school_ids': ','.join(map(str, r.school_ids)) if r.school_ids else ''
            } for r in no_data
        ])
        no_data_file = f"{output_dir}/locations_no_geocodable_data.csv"
        no_data_df.to_csv(no_data_file, index=False)
        print(f"  ⚠️  Exported {len(no_data):,} locations with no geocodable data to {no_data_file}")

def generate_geocoding_recommendations(stats, coordinates_needed, county_needed, no_data):
    """Generate recommendations for geocoding process"""
    print("\n" + "="*80)
    print("💡 GEOCODING RECOMMENDATIONS")
    print("="*80)
    
    total_needing_work = len(coordinates_needed) + len(county_needed)
    
    if total_needing_work == 0:
        print("🎉 EXCELLENT! All active locations have complete geocoding data.")
        print("   No geocoding work needed.")
        return
    
    print(f"📋 GEOCODING WORK NEEDED:")
    
    if coordinates_needed:
        print(f"1. 🗺️  COORDINATE GEOCODING: {len(coordinates_needed):,} locations")
        print(f"   - These locations have address data but missing lat/lng coordinates")
        print(f"   - Use Google Maps Geocoding API to get coordinates")
        print(f"   - Priority: HIGH (coordinates needed for mapping)")
    
    if county_needed:
        print(f"2. 🏛️  COUNTY GEOCODING: {len(county_needed):,} locations")
        print(f"   - These locations need county information")
        print(f"   - Can use reverse geocoding if coordinates exist, or address geocoding")
        print(f"   - Priority: MEDIUM (county data useful for analysis)")
    
    if no_data:
        print(f"3. ❌ NO GEOCODABLE DATA: {len(no_data):,} locations")
        print(f"   - These locations have no address, coordinates, or other location data")
        print(f"   - Manual intervention may be required")
        print(f"   - Priority: LOW (may need data source investigation)")
    
    # Estimate API calls needed
    api_calls_needed = len(coordinates_needed) + len([r for r in county_needed if not (r.latitude and r.longitude)])
    
    print(f"\n💰 ESTIMATED GOOGLE MAPS API CALLS NEEDED: {api_calls_needed:,}")
    print(f"   - Current Google Maps pricing: ~$5 per 1,000 requests")
    print(f"   - Estimated cost: ~${api_calls_needed * 0.005:.2f}")
    
    print(f"\n📝 NEXT STEPS:")
    print(f"1. Review the exported CSV files to understand the data")
    print(f"2. Run 02_geocode_process.py to process the locations")
    print(f"3. Monitor API usage and costs during processing")
    print(f"4. Validate results after geocoding completion")

def main():
    global proxy_process
    
    parser = argparse.ArgumentParser(description='Analyze Geocoding Needs for Location Points')
    parser.add_argument('--export', '-e', action='store_true', 
                       help='Export results to CSV files')
    args = parser.parse_args()
    
    try:
        print("🔍 GEOCODING ANALYSIS")
        print("="*80)
        print("Analyzing location_points to identify geocoding needs")
        print("(coordinates and county data)")
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"⚠️  Service account file not found: {SERVICE_ACCOUNT_FILE}")
            print("Please ensure the service account key file is in the correct location.")
            return 1
        
        # Start Cloud SQL Proxy
        proxy_process, port = start_cloud_sql_proxy()
        
        # Test connection
        success, engine = test_connection(port)
        if not success:
            return 1
        
        # Run analysis
        stats = analyze_geocoding_needs(engine)
        coordinates_needed = identify_coordinates_needed(engine)
        county_needed = identify_county_needed(engine)
        no_data = identify_no_location_data(engine)
        
        # Export results if requested
        if args.export:
            export_geocoding_lists(coordinates_needed, county_needed, no_data)
        
        # Generate recommendations
        generate_geocoding_recommendations(stats, coordinates_needed, county_needed, no_data)
        
        print(f"\n🎉 Geocoding analysis completed successfully!")
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
    print("="*80)
    print("GEOCODING NEEDS ANALYSIS")
    print("="*80)
    result = main()
    print("="*80)
    sys.exit(result)