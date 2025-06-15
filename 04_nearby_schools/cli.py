#!/usr/bin/env python3
"""
Command Line Interface for Nearby Schools Processor

Usage:
    python nearby_schools/cli.py process-location --location-id 1 --data-year 2024
    python nearby_schools/cli.py process-multiple --location-ids 1,2,3,4,5 --data-year 2024
    python nearby_schools/cli.py check-data --data-year 2024
    python nearby_schools/cli.py list-school-locations --data-year 2024
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from nearby_schools.processor import (
    start_cloud_sql_proxy, 
    stop_cloud_sql_proxy, 
    create_connection,
    process_location,
    process_multiple_locations,
    get_processing_summary,
    get_schools_with_locations
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cmd_process_location(args):
    """Process nearby schools for a single location"""
    proxy_process = None
    
    try:
        logger.info(f"🚀 Processing nearby schools for location: {args.location_id}, year: {args.data_year}")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Process the location
        success = process_location(engine, args.location_id, args.data_year, args.force_refresh)
        
        if success:
            logger.info(f"✅ Successfully processed location_id {args.location_id}")
            return 0
        else:
            logger.error(f"❌ Failed to process location_id {args.location_id}")
            return 1
            
    except Exception as e:
        logger.error(f"Error processing location: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def cmd_process_multiple(args):
    """Process nearby schools for multiple locations"""
    proxy_process = None
    
    try:
        # Parse location IDs
        location_ids = [int(x.strip()) for x in args.location_ids.split(',')]
        logger.info(f"🚀 Processing {len(location_ids)} locations: {location_ids}, year: {args.data_year}")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Process the locations
        success_count = process_multiple_locations(engine, location_ids, args.data_year, args.force_refresh)
        
        logger.info(f"✅ Successfully processed {success_count}/{len(location_ids)} locations")
        
        if success_count == len(location_ids):
            return 0
        elif success_count > 0:
            return 2  # Partial success
        else:
            return 1  # Complete failure
            
    except ValueError as e:
        logger.error(f"Invalid location IDs format: {args.location_ids}")
        logger.error("Please provide comma-separated integers, e.g., '1,2,3,4,5'")
        return 1
    except Exception as e:
        logger.error(f"Error processing locations: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def cmd_check_data(args):
    """Check existing nearby schools data"""
    proxy_process = None
    
    try:
        logger.info(f"🔍 Checking nearby schools data for year {args.data_year}...")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Get processing summary
        summary = get_processing_summary(engine, args.data_year)
        
        if summary and summary['total_relationships'] > 0:
            logger.info(f"📊 Nearby Schools Summary for {summary['data_year']}:")
            logger.info(f"  📈 Total polygon relationships: {summary['total_relationships']:,}")
            logger.info(f"  📍 Unique locations processed: {summary['unique_locations']:,}")
            logger.info(f"  🏫 Total nearby schools found: {summary['total_nearby_schools']:,}")
            logger.info(f"  🎯 Unique schools identified: {summary['unique_schools']:,}")
            logger.info(f"  🕐 Drive times processed: {summary['drive_times_processed']}")
            
            if summary['earliest_processed'] and summary['latest_processed']:
                logger.info(f"  📅 Processing range: {summary['earliest_processed']} to {summary['latest_processed']}")
            
            # Show drive time breakdown
            if summary['drive_time_breakdown']:
                logger.info(f"📋 Drive Time Breakdown:")
                for dt_info in summary['drive_time_breakdown']:
                    logger.info(f"  {dt_info['drive_time']} minutes: "
                              f"{dt_info['polygon_count']} polygons, "
                              f"{dt_info['nearby_school_count']} nearby schools")
        else:
            logger.info("📭 No nearby schools data found for the specified year")
            
        return 0
        
    except Exception as e:
        logger.error(f"Error checking data: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def cmd_list_school_locations(args):
    """List schools with locations for a given data year"""
    proxy_process = None
    
    try:
        logger.info(f"📍 Fetching schools with locations for year {args.data_year}...")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Get schools with locations
        schools = get_schools_with_locations(engine, args.data_year)
        
        if schools:
            logger.info(f"📍 Found {len(schools)} schools with locations for year {args.data_year}:")
            
            # Group by location_id for summary
            location_groups = {}
            for school in schools:
                loc_id = school['location_id']
                if loc_id not in location_groups:
                    location_groups[loc_id] = []
                location_groups[loc_id].append(school)
            
            # Show first 20 locations to avoid overwhelming output
            shown_count = 0
            for location_id, school_list in list(location_groups.items())[:20]:
                first_school = school_list[0]
                logger.info(f"  Location {location_id}: {first_school['latitude']:.6f}, {first_school['longitude']:.6f}")
                logger.info(f"    {len(school_list)} school(s): {', '.join([s['school_uuid'][:8] + '...' for s in school_list])}")
                shown_count += 1
                
            if len(location_groups) > 20:
                logger.info(f"  ... and {len(location_groups) - 20} more locations")
                
            logger.info(f"📊 Summary: {len(location_groups)} unique locations, {len(schools)} total schools")
        else:
            logger.info(f"📭 No schools with locations found for year {args.data_year}")
            
        return 0
        
    except Exception as e:
        logger.error(f"Error listing school locations: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def cmd_locations_with_esri(args):
    """List locations that have ESRI data and can be processed"""
    proxy_process = None
    
    try:
        logger.info("🗺️  Checking locations with ESRI polygon data...")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Get locations with ESRI polygon data
            query = text("""
                SELECT 
                    location_id,
                    COUNT(DISTINCT drive_time) as drive_time_count,
                    ARRAY_AGG(DISTINCT drive_time ORDER BY drive_time) as drive_times,
                    MAX(processed_at) as last_processed
                FROM esri_demographic_data 
                WHERE drive_time_polygon IS NOT NULL
                GROUP BY location_id
                ORDER BY location_id
                LIMIT 50
            """)
            
            results = conn.execute(query).fetchall()
            
            if results:
                logger.info(f"🗺️  Found {len(results)} locations with ESRI polygon data:")
                for row in results:
                    location_id = row[0]
                    drive_time_count = row[1]
                    drive_times = row[2]
                    last_processed = row[3]
                    
                    drive_times_str = ','.join(map(str, drive_times)) if drive_times else 'None'
                    logger.info(f"  Location {location_id}: {drive_time_count} drive times [{drive_times_str}], "
                              f"processed {last_processed}")
                              
                if len(results) == 50:
                    logger.info("  ... (showing first 50 locations)")
            else:
                logger.info("📭 No locations with ESRI polygon data found")
                logger.info("Run ESRI demographic processing first to generate polygon data")
                
        return 0
        
    except Exception as e:
        logger.error(f"Error checking ESRI locations: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def main():
    parser = argparse.ArgumentParser(
        description="Nearby Schools Processor CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s process-location --location-id 1 --data-year 2024
  %(prog)s process-location --location-id 5 --data-year 2024 --force-refresh
  %(prog)s process-multiple --location-ids "1,2,3,4,5" --data-year 2024
  %(prog)s check-data --data-year 2024
  %(prog)s list-school-locations --data-year 2024
  %(prog)s locations-with-esri
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Process single location
    single_parser = subparsers.add_parser('process-location', help='Process nearby schools for a single location')
    single_parser.add_argument('--location-id', type=int, required=True,
                             help='Location ID to process')
    single_parser.add_argument('--data-year', type=int, default=2024,
                             help='Data year to process (default: 2024)')
    single_parser.add_argument('--force-refresh', action='store_true',
                             help='Force refresh even if data already exists')
    single_parser.set_defaults(func=cmd_process_location)
    
    # Process multiple locations
    multiple_parser = subparsers.add_parser('process-multiple', help='Process nearby schools for multiple locations')
    multiple_parser.add_argument('--location-ids', type=str, required=True,
                               help='Comma-separated list of location IDs (e.g., "1,2,3,4,5")')
    multiple_parser.add_argument('--data-year', type=int, default=2024,
                               help='Data year to process (default: 2024)')
    multiple_parser.add_argument('--force-refresh', action='store_true',
                               help='Force refresh even if data already exists')
    multiple_parser.set_defaults(func=cmd_process_multiple)
    
    # Check data
    check_parser = subparsers.add_parser('check-data', help='Check existing nearby schools data')
    check_parser.add_argument('--data-year', type=int, default=2024,
                            help='Data year to check (default: 2024)')
    check_parser.set_defaults(func=cmd_check_data)
    
    # List school locations
    list_parser = subparsers.add_parser('list-school-locations', help='List schools with locations')
    list_parser.add_argument('--data-year', type=int, default=2024,
                           help='Data year to list (default: 2024)')
    list_parser.set_defaults(func=cmd_list_school_locations)
    
    # List locations with ESRI data
    esri_parser = subparsers.add_parser('locations-with-esri', help='List locations that have ESRI polygon data')
    esri_parser.set_defaults(func=cmd_locations_with_esri)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        return args.func(args)
    except KeyboardInterrupt:
        logger.info("❌ Process interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    print("="*60)
    print("NEARBY SCHOOLS PROCESSOR CLI")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result) 