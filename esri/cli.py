#!/usr/bin/env python3
"""
Command Line Interface for ESRI Unified Processor

Usage:
    python esri/cli.py process-single --location-id 1
    python esri/cli.py process-multiple --location-ids 1,2,3,4,5
    python esri/cli.py check-data
    python esri/cli.py list-locations
"""

import argparse
import sys
import logging
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from esri.unified_processor import (
    start_cloud_sql_proxy, 
    stop_cloud_sql_proxy, 
    create_connection,
    create_demographic_table,
    process_location,
    process_multiple_locations
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cmd_process_single(args):
    """Process a single location"""
    proxy_process = None
    
    try:
        logger.info(f"🚀 Processing single location: {args.location_id}")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Ensure table exists
        create_demographic_table(engine)
        
        # Process the location
        success = process_location(engine, args.location_id, force_refresh=args.force_refresh)
        
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
    """Process multiple locations"""
    proxy_process = None
    
    try:
        # Parse location IDs
        location_ids = [int(x.strip()) for x in args.location_ids.split(',')]
        logger.info(f"🚀 Processing {len(location_ids)} locations: {location_ids}")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Ensure table exists
        create_demographic_table(engine)
        
        # Process the locations
        success_count = process_multiple_locations(engine, location_ids, force_refresh=args.force_refresh)
        
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
    """Check existing ESRI data"""
    proxy_process = None
    
    try:
        logger.info("🔍 Checking existing ESRI data...")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Check if table exists
            table_check = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'esri_demographic_data'
                );
            """)).scalar()
            
            if not table_check:
                logger.info("📭 No esri_demographic_data table found. Run a process command first.")
                return 0
            
            # Get summary statistics
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT location_id) as unique_locations,
                    MIN(processed_at) as earliest_data,
                    MAX(processed_at) as latest_data,
                    COUNT(CASE WHEN processed_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_records
                FROM esri_demographic_data
            """)).fetchone()
            
            if result and result[0] > 0:
                logger.info(f"📊 Database Summary:")
                logger.info(f"  📈 Total records: {result[0]:,}")
                logger.info(f"  📍 Unique locations: {result[1]:,}")
                logger.info(f"  📅 Data range: {result[2]} to {result[3]}")
                logger.info(f"  🆕 Recent records (7 days): {result[4]:,}")
                
                # Show locations with data
                locations = conn.execute(text("""
                    SELECT location_id, 
                           COUNT(*) as record_count,
                           MAX(processed_at) as last_updated,
                           ARRAY_AGG(DISTINCT drive_time ORDER BY drive_time) as drive_times
                    FROM esri_demographic_data 
                    GROUP BY location_id 
                    ORDER BY MAX(processed_at) DESC 
                    LIMIT 10
                """)).fetchall()
                
                logger.info(f"📋 Recent locations (showing up to 10):")
                for row in locations:
                    drive_times_str = ','.join(map(str, row[3]))
                    logger.info(f"  Location {row[0]}: {row[1]} records, "
                              f"drive times [{drive_times_str}], "
                              f"updated {row[2]}")
                              
                # Show sample demographic data
                sample = conn.execute(text("""
                    SELECT location_id, drive_time, 
                           ROUND(per_hisp_child_20::numeric, 3) as hispanic_pct,
                           ROUND(per_wht_child_20::numeric, 3) as white_pct,
                           ROUND(medhinc_cy::numeric, 0) as median_income
                    FROM esri_demographic_data 
                    WHERE per_hisp_child_20 IS NOT NULL 
                      AND per_wht_child_20 IS NOT NULL 
                      AND medhinc_cy IS NOT NULL
                    ORDER BY processed_at DESC 
                    LIMIT 5
                """)).fetchall()
                
                if sample:
                    logger.info(f"📊 Sample demographic data:")
                    for row in sample:
                        logger.info(f"  Location {row[0]} ({row[1]}min): "
                                  f"Hispanic {row[2]:.1%}, White {row[3]:.1%}, "
                                  f"Income ${row[4]:,.0f}")
            else:
                logger.info("📭 No ESRI demographic data found in database")
                
        return 0
        
    except Exception as e:
        logger.error(f"Error checking data: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def cmd_list_locations(args):
    """List available locations from location_points table"""
    proxy_process = None
    
    try:
        logger.info("📍 Fetching available locations...")
        
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Get locations
            result = conn.execute(text("""
                SELECT location_id, latitude, longitude
                FROM location_points 
                ORDER BY location_id 
                LIMIT 50
            """)).fetchall()
            
            if result:
                logger.info(f"📍 Found {len(result)} locations:")
                for row in result:
                    logger.info(f"  Location {row[0]}: {row[1]:.6f}, {row[2]:.6f}")
                    
                if len(result) == 50:
                    logger.info("  ... (showing first 50 locations)")
            else:
                logger.info("📭 No locations found in location_points table")
                
        return 0
        
    except Exception as e:
        logger.error(f"Error listing locations: {str(e)}")
        return 1
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def main():
    parser = argparse.ArgumentParser(
        description="ESRI Unified Processor CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s process-single --location-id 1
  %(prog)s process-single --location-id 5 --force-refresh
  %(prog)s process-multiple --location-ids "1,2,3,4,5"
  %(prog)s check-data
  %(prog)s list-locations
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Process single location
    single_parser = subparsers.add_parser('process-single', help='Process a single location')
    single_parser.add_argument('--location-id', type=int, required=True,
                             help='Location ID to process')
    single_parser.add_argument('--force-refresh', action='store_true',
                             help='Force refresh even if recent data exists')
    single_parser.set_defaults(func=cmd_process_single)
    
    # Process multiple locations
    multiple_parser = subparsers.add_parser('process-multiple', help='Process multiple locations')
    multiple_parser.add_argument('--location-ids', type=str, required=True,
                               help='Comma-separated list of location IDs (e.g., "1,2,3,4,5")')
    multiple_parser.add_argument('--force-refresh', action='store_true',
                               help='Force refresh even if recent data exists')
    multiple_parser.set_defaults(func=cmd_process_multiple)
    
    # Check data
    check_parser = subparsers.add_parser('check-data', help='Check existing ESRI data')
    check_parser.set_defaults(func=cmd_check_data)
    
    # List locations
    list_parser = subparsers.add_parser('list-locations', help='List available locations')
    list_parser.set_defaults(func=cmd_list_locations)
    
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
    print("ESRI UNIFIED PROCESSOR CLI")
    print("="*60)
    result = main()
    print("="*60)
    sys.exit(result) 