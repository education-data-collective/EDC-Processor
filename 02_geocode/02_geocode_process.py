#!/usr/bin/env python3
"""
Geocoding Processing Script
Processes location_points using Google Maps API to fill in missing coordinates and county data
"""

import os
import sys
import subprocess
import time
import socket
import signal
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
from dotenv import load_dotenv
import json
import requests
from datetime import datetime
import random

# Load environment variables
load_dotenv()

try:
    from sqlalchemy import create_engine, text
    import googlemaps
except ImportError:
    print("Required packages not found. Please install with:")
    print("pip install sqlalchemy googlemaps")
    sys.exit(1)

# Configuration
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = '../etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'

# Google Maps API configuration
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
if not GOOGLE_MAPS_API_KEY:
    print("❌ GOOGLE_MAPS_API_KEY not found in environment variables")
    print("Please make sure your .env file contains the API key")
    sys.exit(1)

# Rate limiting configuration
MAX_REQUESTS_PER_SECOND = 50  # Google Maps API limit
REQUEST_DELAY = 1.0 / MAX_REQUESTS_PER_SECOND  # Delay between requests
BATCH_SIZE = 100  # Process locations in batches
SAVE_PROGRESS_EVERY = 25  # Save progress every N locations

# Global variables for cleanup
proxy_process = None
gmaps_client = None

def signal_handler(signum, frame):
    global proxy_process
    print(f"\nReceived signal {signum}. Cleaning up...")
    if proxy_process:
        stop_cloud_sql_proxy(proxy_process)
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path("01_geocode/logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"geocoding_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

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

def initialize_gmaps_client():
    """Initialize Google Maps client"""
    global gmaps_client
    try:
        gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
        
        # Test the connection with a simple request
        result = gmaps_client.geocode("1600 Amphitheatre Parkway, Mountain View, CA")
        if result:
            print("✅ Google Maps API connection successful!")
            return True
        else:
            print("❌ Google Maps API test failed - no results returned")
            return False
            
    except Exception as e:
        print(f"❌ Google Maps API initialization failed: {str(e)}")
        return False

def build_address_string(address, city, state, zip_code):
    """Build a complete address string from components"""
    parts = []
    
    if address and address.strip():
        parts.append(address.strip())
    
    if city and city.strip():
        parts.append(city.strip())
    
    if state and state.strip():
        parts.append(state.strip())
    
    if zip_code and str(zip_code).strip():
        parts.append(str(zip_code).strip())
    
    return ', '.join(parts) if parts else None

def geocode_address(address_string, logger):
    """Geocode an address using Google Maps API"""
    if not address_string:
        return None, "No address data"
    
    try:
        # Add delay for rate limiting
        time.sleep(REQUEST_DELAY)
        
        # Geocode the address
        result = gmaps_client.geocode(address_string)
        
        if not result:
            logger.warning(f"No results for address: {address_string}")
            return None, "No results found"
        
        # Extract the first (best) result
        location = result[0]
        geometry = location.get('geometry', {})
        location_data = geometry.get('location', {})
        
        if not location_data:
            logger.warning(f"No location data in result for: {address_string}")
            return None, "No location data in result"
        
        # Extract coordinates
        lat = location_data.get('lat')
        lng = location_data.get('lng')
        
        if lat is None or lng is None:
            logger.warning(f"Missing coordinates in result for: {address_string}")
            return None, "Missing coordinates"
        
        # Extract county from address components
        county = None
        for component in location.get('address_components', []):
            if 'administrative_area_level_2' in component.get('types', []):
                county = component.get('long_name', '').replace(' County', '')
                break
        
        # Extract other useful info
        formatted_address = location.get('formatted_address', address_string)
        place_id = location.get('place_id')
        
        result_data = {
            'latitude': lat,
            'longitude': lng,
            'county': county,
            'formatted_address': formatted_address,
            'place_id': place_id,
            'raw_result': json.dumps(location)
        }
        
        logger.info(f"Successfully geocoded: {address_string} -> ({lat}, {lng}) | County: {county}")
        return result_data, "Success"
        
    except googlemaps.exceptions.ApiError as e:
        logger.error(f"Google Maps API error for {address_string}: {str(e)}")
        return None, f"API Error: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error geocoding {address_string}: {str(e)}")
        return None, f"Error: {str(e)}"

def reverse_geocode_coordinates(lat, lng, logger):
    """Reverse geocode coordinates to get county information"""
    try:
        # Add delay for rate limiting
        time.sleep(REQUEST_DELAY)
        
        # Reverse geocode the coordinates
        result = gmaps_client.reverse_geocode((lat, lng))
        
        if not result:
            logger.warning(f"No reverse geocoding results for coordinates: ({lat}, {lng})")
            return None, "No results found"
        
        # Extract county from the first result
        location = result[0]
        county = None
        
        for component in location.get('address_components', []):
            if 'administrative_area_level_2' in component.get('types', []):
                county = component.get('long_name', '').replace(' County', '')
                break
        
        if county:
            logger.info(f"Successfully reverse geocoded ({lat}, {lng}) -> County: {county}")
            return {'county': county}, "Success"
        else:
            logger.warning(f"No county found in reverse geocoding result for ({lat}, {lng})")
            return None, "No county found"
            
    except googlemaps.exceptions.ApiError as e:
        logger.error(f"Google Maps API error for reverse geocoding ({lat}, {lng}): {str(e)}")
        return None, f"API Error: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error reverse geocoding ({lat}, {lng}): {str(e)}")
        return None, f"Error: {str(e)}"

def get_locations_needing_coordinates(engine, limit=None):
    """Get locations that need coordinate geocoding"""
    query = """
        SELECT 
            lp.id as location_point_id,
            lp.address,
            lp.city,
            lp.state,
            lp.zip_code,
            lp.county,
            COUNT(sl.school_id) as school_count
        FROM location_points lp
        INNER JOIN school_locations sl ON lp.id = sl.location_id
        WHERE (lp.latitude IS NULL OR lp.longitude IS NULL)
          AND ((lp.address IS NOT NULL AND lp.address != '') 
               OR (lp.city IS NOT NULL AND lp.city != '')
               OR (lp.zip_code IS NOT NULL AND lp.zip_code != ''))
        GROUP BY lp.id, lp.address, lp.city, lp.state, lp.zip_code, lp.county
        ORDER BY school_count DESC, lp.id
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

def get_locations_needing_county(engine, limit=None):
    """Get locations that need county geocoding"""
    query = """
        SELECT 
            lp.id as location_point_id,
            lp.address,
            lp.city,
            lp.state,
            lp.zip_code,
            lp.latitude,
            lp.longitude,
            COUNT(sl.school_id) as school_count
        FROM location_points lp
        INNER JOIN school_locations sl ON lp.id = sl.location_id
        WHERE (lp.county IS NULL OR lp.county = '')
          AND ((lp.address IS NOT NULL AND lp.address != '') 
               OR (lp.city IS NOT NULL AND lp.city != '')
               OR (lp.zip_code IS NOT NULL AND lp.zip_code != '')
               OR (lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL))
        GROUP BY lp.id, lp.address, lp.city, lp.state, lp.zip_code, lp.latitude, lp.longitude
        ORDER BY school_count DESC, lp.id
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

def update_location_coordinates(engine, location_id, lat, lng, county, formatted_address, logger):
    """Update location_point with geocoded coordinates and county"""
    try:
        with engine.begin() as conn:
            update_parts = []
            params = {'location_id': location_id}
            
            if lat is not None and lng is not None:
                update_parts.append("latitude = :latitude, longitude = :longitude")
                params['latitude'] = lat
                params['longitude'] = lng
            
            if county:
                update_parts.append("county = :county")
                params['county'] = county
            
            # Note: No updated_at column in location_points table
            
            if update_parts:
                query = f"""
                    UPDATE location_points 
                    SET {', '.join(update_parts)}
                    WHERE id = :location_id
                """
                
                result = conn.execute(text(query), params)
                
                if result.rowcount == 1:
                    logger.info(f"Updated location {location_id} with coordinates ({lat}, {lng}) and county '{county}'")
                    return True
                else:
                    logger.warning(f"No rows updated for location {location_id}")
                    return False
            else:
                logger.warning(f"No updates to apply for location {location_id}")
                return False
                
    except Exception as e:
        logger.error(f"Database error updating location {location_id}: {str(e)}")
        return False

def update_location_county(engine, location_id, county, logger):
    """Update location_point with county information"""
    try:
        with engine.begin() as conn:
            query = """
                UPDATE location_points 
                SET county = :county
                WHERE id = :location_id
            """
            
            result = conn.execute(text(query), {
                'location_id': location_id,
                'county': county
            })
            
            if result.rowcount == 1:
                logger.info(f"Updated location {location_id} with county '{county}'")
                return True
            else:
                logger.warning(f"No rows updated for location {location_id}")
                return False
                
    except Exception as e:
        logger.error(f"Database error updating county for location {location_id}: {str(e)}")
        return False

def process_coordinate_geocoding(engine, logger, limit=None, dry_run=False):
    """Process locations needing coordinate geocoding"""
    print("\n" + "="*80)
    print("🗺️  PROCESSING COORDINATE GEOCODING")
    print("="*80)
    
    # Get locations needing coordinates
    locations = get_locations_needing_coordinates(engine, limit)
    
    if not locations:
        print("✅ No locations need coordinate geocoding!")
        return 0, 0, 0
    
    print(f"Found {len(locations):,} locations needing coordinate geocoding")
    if dry_run:
        print("🔍 DRY RUN - No actual updates will be made")
    
    success_count = 0
    error_count = 0
    api_calls = 0
    
    for i, location in enumerate(locations, 1):
        location_id = location.location_point_id
        
        # Build address string
        address_string = build_address_string(
            location.address, 
            location.city, 
            location.state, 
            location.zip_code
        )
        
        print(f"\n[{i}/{len(locations)}] Processing location {location_id}")
        print(f"  Address: {address_string}")
        print(f"  Used by {location.school_count} school(s)")
        
        if not address_string:
            print("  ❌ No address data to geocode")
            error_count += 1
            continue
        
        # Geocode the address
        geocode_result, error_msg = geocode_address(address_string, logger)
        api_calls += 1
        
        if geocode_result:
            if not dry_run:
                # Update the database
                update_success = update_location_coordinates(
                    engine,
                    location_id,
                    geocode_result['latitude'],
                    geocode_result['longitude'],
                    geocode_result['county'],
                    geocode_result['formatted_address'],
                    logger
                )
                
                if update_success:
                    success_count += 1
                    print(f"  ✅ Updated: ({geocode_result['latitude']:.6f}, {geocode_result['longitude']:.6f})")
                    if geocode_result['county']:
                        print(f"     County: {geocode_result['county']}")
                else:
                    error_count += 1
                    print("  ❌ Database update failed")
            else:
                success_count += 1
                print(f"  ✅ Would update: ({geocode_result['latitude']:.6f}, {geocode_result['longitude']:.6f})")
                if geocode_result['county']:
                    print(f"     County: {geocode_result['county']}")
        else:
            error_count += 1
            print(f"  ❌ Geocoding failed: {error_msg}")
        
        # Save progress periodically
        if i % SAVE_PROGRESS_EVERY == 0:
            print(f"\n📊 Progress: {i}/{len(locations)} processed | {success_count} successful | {error_count} errors | {api_calls} API calls")
    
    return success_count, error_count, api_calls

def process_county_geocoding(engine, logger, limit=None, dry_run=False):
    """Process locations needing county geocoding"""
    print("\n" + "="*80)
    print("🏛️  PROCESSING COUNTY GEOCODING")
    print("="*80)
    
    # Get locations needing county data
    locations = get_locations_needing_county(engine, limit)
    
    if not locations:
        print("✅ No locations need county geocoding!")
        return 0, 0, 0
    
    print(f"Found {len(locations):,} locations needing county geocoding")
    if dry_run:
        print("🔍 DRY RUN - No actual updates will be made")
    
    success_count = 0
    error_count = 0
    api_calls = 0
    
    for i, location in enumerate(locations, 1):
        location_id = location.location_point_id
        
        print(f"\n[{i}/{len(locations)}] Processing location {location_id}")
        print(f"  Used by {location.school_count} school(s)")
        
        county_result = None
        error_msg = None
        
        # Try reverse geocoding if we have coordinates
        if location.latitude and location.longitude:
            print(f"  Reverse geocoding coordinates: ({location.latitude:.6f}, {location.longitude:.6f})")
            county_result, error_msg = reverse_geocode_coordinates(location.latitude, location.longitude, logger)
            api_calls += 1
        else:
            # Try address geocoding
            address_string = build_address_string(
                location.address, 
                location.city, 
                location.state, 
                location.zip_code
            )
            
            if address_string:
                print(f"  Geocoding address: {address_string}")
                geocode_result, error_msg = geocode_address(address_string, logger)
                api_calls += 1
                
                if geocode_result and geocode_result.get('county'):
                    county_result = {'county': geocode_result['county']}
            else:
                error_msg = "No coordinates or address data available"
        
        if county_result and county_result.get('county'):
            if not dry_run:
                # Update the database
                update_success = update_location_county(
                    engine,
                    location_id,
                    county_result['county'],
                    logger
                )
                
                if update_success:
                    success_count += 1
                    print(f"  ✅ Updated county: {county_result['county']}")
                else:
                    error_count += 1
                    print("  ❌ Database update failed")
            else:
                success_count += 1
                print(f"  ✅ Would update county: {county_result['county']}")
        else:
            error_count += 1
            print(f"  ❌ County geocoding failed: {error_msg}")
        
        # Save progress periodically
        if i % SAVE_PROGRESS_EVERY == 0:
            print(f"\n📊 Progress: {i}/{len(locations)} processed | {success_count} successful | {error_count} errors | {api_calls} API calls")
    
    return success_count, error_count, api_calls

def validate_geocoding_results(engine, logger):
    """Validate the geocoding results"""
    print("\n" + "="*80)
    print("✅ VALIDATING GEOCODING RESULTS")
    print("="*80)
    
    with engine.connect() as conn:
        # Check overall completion rates
        result = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT lp.id) as total_active_locations,
                COUNT(DISTINCT CASE WHEN lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL THEN lp.id END) as has_coordinates,
                COUNT(DISTINCT CASE WHEN lp.county IS NOT NULL AND lp.county != '' THEN lp.id END) as has_county,
                COUNT(DISTINCT CASE WHEN lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL 
                                     AND lp.county IS NOT NULL AND lp.county != '' THEN lp.id END) as fully_geocoded
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
        """))
        
        stats = result.fetchone()
        
        print("📊 GEOCODING COMPLETION RATES:")
        print(f"  📍 Total active locations: {stats.total_active_locations:,}")
        print(f"  🗺️  Has coordinates: {stats.has_coordinates:,} ({100.0 * stats.has_coordinates / stats.total_active_locations:.1f}%)")
        print(f"  🏛️  Has county: {stats.has_county:,} ({100.0 * stats.has_county / stats.total_active_locations:.1f}%)")
        print(f"  ✅ Fully geocoded: {stats.fully_geocoded:,} ({100.0 * stats.fully_geocoded / stats.total_active_locations:.1f}%)")
        
        # Check for any obviously invalid coordinates (outside US bounds roughly)
        result = conn.execute(text("""
            SELECT COUNT(*) as invalid_coords
            FROM location_points lp
            INNER JOIN school_locations sl ON lp.id = sl.location_id
            WHERE lp.latitude IS NOT NULL AND lp.longitude IS NOT NULL
              AND (lp.latitude < 20 OR lp.latitude > 70 OR lp.longitude < -180 OR lp.longitude > -50)
        """))
        
        invalid_coords = result.scalar()
        
        if invalid_coords > 0:
            print(f"⚠️  Found {invalid_coords} locations with potentially invalid coordinates (outside typical US bounds)")
        else:
            print("✅ All coordinates appear to be within valid US bounds")
        
        return stats

def main():
    global proxy_process
    
    parser = argparse.ArgumentParser(description='Process Geocoding for Location Points')
    parser.add_argument('--coordinates', '-c', action='store_true', 
                       help='Process coordinate geocoding')
    parser.add_argument('--county', '-n', action='store_true', 
                       help='Process county geocoding')
    parser.add_argument('--all', '-a', action='store_true', 
                       help='Process both coordinate and county geocoding')
    parser.add_argument('--limit', '-l', type=int, 
                       help='Limit number of locations to process (for testing)')
    parser.add_argument('--dry-run', '-d', action='store_true', 
                       help='Dry run - show what would be done without making changes')
    parser.add_argument('--validate', '-v', action='store_true', 
                       help='Validate geocoding results only')
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    try:
        print("🌐 GEOCODING PROCESSOR")
        print("="*80)
        print("Processing location_points with Google Maps API")
        
        if args.dry_run:
            print("🔍 DRY RUN MODE - No actual changes will be made")
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"⚠️  Service account file not found: {SERVICE_ACCOUNT_FILE}")
            print("Please ensure the service account key file is in the correct location.")
            return 1
        
        # Initialize Google Maps client
        if not args.validate:
            if not initialize_gmaps_client():
                return 1
        
        # Start Cloud SQL Proxy
        proxy_process, port = start_cloud_sql_proxy()
        
        # Test connection
        success, engine = test_connection(port)
        if not success:
            return 1
        
        # Process based on arguments
        total_success = 0
        total_errors = 0
        total_api_calls = 0
        
        if args.validate:
            validate_geocoding_results(engine, logger)
        elif args.all or args.coordinates:
            # Process coordinate geocoding
            success, errors, api_calls = process_coordinate_geocoding(
                engine, logger, args.limit, args.dry_run
            )
            total_success += success
            total_errors += errors
            total_api_calls += api_calls
        
        if args.all or args.county:
            # Process county geocoding
            success, errors, api_calls = process_county_geocoding(
                engine, logger, args.limit, args.dry_run
            )
            total_success += success
            total_errors += errors
            total_api_calls += api_calls
        
        if not args.validate:
            # Final validation
            validate_geocoding_results(engine, logger)
            
            # Summary
            print(f"\n🎉 GEOCODING PROCESSING COMPLETED!")
            print(f"📊 SUMMARY:")
            print(f"  ✅ Successful updates: {total_success:,}")
            print(f"  ❌ Errors: {total_errors:,}")
            print(f"  🔌 API calls made: {total_api_calls:,}")
            
            if total_api_calls > 0:
                estimated_cost = total_api_calls * 0.005
                print(f"  💰 Estimated cost: ${estimated_cost:.2f}")
        
        if not (args.coordinates or args.county or args.all or args.validate):
            print("\n💡 Please specify what to process:")
            print("  --coordinates (-c) : Process coordinate geocoding")
            print("  --county (-n)      : Process county geocoding") 
            print("  --all (-a)         : Process both")
            print("  --validate (-v)    : Validate results only")
            print("  --dry-run (-d)     : Dry run mode")
            print("  --limit N (-l N)   : Limit to N locations")
        
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
    print("GEOCODING PROCESSOR")
    print("="*80)
    result = main()
    print("="*80)
    sys.exit(result)