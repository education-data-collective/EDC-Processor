#!/usr/bin/env python3
"""
Example Usage of ESRI Unified Processor

This script demonstrates how to use the unified processor to fetch and store ESRI data.
"""

import sys
from pathlib import Path

# Add the parent directory to the path so we can import the unified processor
sys.path.append(str(Path(__file__).parent.parent))

from esri.unified_processor import (
    start_cloud_sql_proxy, 
    stop_cloud_sql_proxy, 
    create_connection,
    create_demographic_table,
    process_location,
    process_multiple_locations
)
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def example_single_location():
    """Example: Process a single location"""
    proxy_process = None
    
    try:
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Ensure table exists
        create_demographic_table(engine)
        
        # Process a single location
        location_id = 1
        success = process_location(engine, location_id, force_refresh=True)
        
        if success:
            logger.info(f"✅ Successfully processed location_id {location_id}")
        else:
            logger.error(f"❌ Failed to process location_id {location_id}")
            
        return success
        
    except Exception as e:
        logger.error(f"Error in example_single_location: {str(e)}")
        return False
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def example_multiple_locations():
    """Example: Process multiple locations"""
    proxy_process = None
    
    try:
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Ensure table exists
        create_demographic_table(engine)
        
        # Process multiple locations
        location_ids = [1, 2, 3, 4, 5]  # Change these to your actual location_ids
        success_count = process_multiple_locations(engine, location_ids, force_refresh=False)
        
        logger.info(f"✅ Successfully processed {success_count}/{len(location_ids)} locations")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Error in example_multiple_locations: {str(e)}")
        return False
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

def example_check_existing_data():
    """Example: Check what data already exists"""
    proxy_process = None
    
    try:
        # Start proxy and create connection
        proxy_process, port = start_cloud_sql_proxy()
        engine = create_connection(port)
        
        # Check existing data
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Get count of existing records
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT location_id) as unique_locations,
                    MIN(processed_at) as earliest_data,
                    MAX(processed_at) as latest_data
                FROM esri_demographic_data
            """)).fetchone()
            
            if result and result[0] > 0:
                logger.info(f"📊 Database contains:")
                logger.info(f"  - Total records: {result[0]:,}")
                logger.info(f"  - Unique locations: {result[1]:,}")
                logger.info(f"  - Data range: {result[2]} to {result[3]}")
                
                # Show sample data
                sample = conn.execute(text("""
                    SELECT location_id, drive_time, latitude, longitude, 
                           per_hisp_child_20, per_wht_child_20, medhinc_cy, processed_at
                    FROM esri_demographic_data 
                    ORDER BY processed_at DESC 
                    LIMIT 5
                """)).fetchall()
                
                logger.info("📋 Sample recent data:")
                for row in sample:
                    logger.info(f"  Location {row[0]}, {row[1]}min: "
                              f"Hispanic {row[4]:.1%}, White {row[5]:.1%}, "
                              f"Income ${row[6]:,} - {row[7]}")
            else:
                logger.info("📭 No ESRI demographic data found in database")
                
        return True
        
    except Exception as e:
        logger.error(f"Error checking existing data: {str(e)}")
        return False
    finally:
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)

if __name__ == "__main__":
    print("="*60)
    print("ESRI PROCESSOR EXAMPLES")
    print("="*60)
    
    print("\n1. Checking existing data...")
    example_check_existing_data()
    
    print("\n2. Processing single location...")
    example_single_location()
    
    # Uncomment the next section if you want to process multiple locations
    # print("\n3. Processing multiple locations...")
    # example_multiple_locations()
    
    print("\n4. Final data check...")
    example_check_existing_data()
    
    print("="*60) 