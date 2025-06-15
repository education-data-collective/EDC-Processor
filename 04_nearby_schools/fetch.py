#!/usr/bin/env python3
"""
Nearby Schools Fetch Module

Core functionality for processing nearby schools using ESRI drive-time polygons.
This module contains the extracted logic from processor.py for school detection
and polygon relationship management.
"""

import json
import logging
from typing import List, Dict, Tuple, Optional

try:
    from sqlalchemy import text
    from shapely.geometry import Point, Polygon
    from shapely.ops import transform
    import shapely.wkt
except ImportError:
    raise ImportError("Required packages not found. Please install: sqlalchemy shapely")

# Set up logging
logger = logging.getLogger(__name__)

def get_esri_polygons(engine, location_id: int) -> Dict[int, Dict]:
    """Get ESRI drive-time polygons for a location"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT drive_time, drive_time_polygon, latitude, longitude
                FROM esri_demographic_data
                WHERE location_id = :location_id
                    AND drive_time_polygon IS NOT NULL
                ORDER BY drive_time
            """)
            
            results = conn.execute(query, {'location_id': location_id}).fetchall()
            
            polygons = {}
            for row in results:
                drive_time = row[0]
                polygon_json = row[1]
                latitude = row[2]
                longitude = row[3]
                
                if polygon_json:
                    try:
                        # Parse the polygon JSON
                        if isinstance(polygon_json, str):
                            polygon_data = json.loads(polygon_json)
                        else:
                            polygon_data = polygon_json
                        
                        # Extract the rings from ESRI format
                        if 'rings' in polygon_data and polygon_data['rings']:
                            # Convert ESRI rings to Shapely polygon
                            exterior_ring = polygon_data['rings'][0]
                            
                            # Create Shapely polygon
                            if len(exterior_ring) >= 4:  # Minimum points for a polygon
                                # ESRI coordinates are [longitude, latitude]
                                coords = [(point[0], point[1]) for point in exterior_ring]
                                shapely_polygon = Polygon(coords)
                                
                                polygons[drive_time] = {
                                    'polygon': shapely_polygon,
                                    'center_lat': latitude,
                                    'center_lon': longitude,
                                    'raw_data': polygon_data
                                }
                                logger.debug(f"Created polygon for drive_time {drive_time} at location {location_id}")
                            else:
                                logger.warning(f"Insufficient points for polygon at location {location_id}, drive_time {drive_time}")
                        else:
                            logger.warning(f"No rings found in polygon data for location {location_id}, drive_time {drive_time}")
                            
                    except Exception as polygon_error:
                        logger.warning(f"Error parsing polygon for location {location_id}, drive_time {drive_time}: {str(polygon_error)}")
                        continue
            
            logger.info(f"Retrieved {len(polygons)} valid polygons for location {location_id}")
            return polygons
            
    except Exception as e:
        logger.error(f"Error getting ESRI polygons for location {location_id}: {str(e)}")
        return {}

def get_all_schools_with_locations(engine, data_year: int) -> List[Dict]:
    """Get all schools with their current locations for a specific data year"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    s.id as school_id,
                    s.uuid as school_uuid,
                    COALESCE(CONCAT(sd.ncessch, '-', sd.split_suffix), sd.ncessch, sd.state_school_id) as external_school_id,
                    sd.system_name as school_name,
                    sl.location_id,
                    lp.latitude,
                    lp.longitude,
                    sl.data_year,
                    sl.school_year
                FROM schools s
                JOIN school_locations sl ON s.id = sl.school_id
                JOIN location_points lp ON sl.location_id = lp.id
                JOIN school_directory sd ON s.id = sd.school_id AND sd.is_current = true
                WHERE sl.data_year = :data_year
                    AND lp.latitude IS NOT NULL 
                    AND lp.longitude IS NOT NULL
                    AND sl.is_current = true
                ORDER BY s.id
            """)
            
            results = conn.execute(query, {'data_year': data_year}).fetchall()
            
            schools = []
            for row in results:
                schools.append({
                    'school_id': row[0],
                    'school_uuid': row[1],
                    'external_school_id': row[2],
                    'school_name': row[3],
                    'location_id': row[4],
                    'latitude': float(row[5]),
                    'longitude': float(row[6]),
                    'data_year': row[7],
                    'school_year': row[8]
                })
            
            logger.info(f"Found {len(schools)} schools with locations for data year {data_year}")
            return schools
            
    except Exception as e:
        logger.error(f"Error getting schools with locations: {str(e)}")
        return []

def find_nearby_schools(target_location_id: int, target_polygons: Dict, all_schools: List[Dict]) -> Dict[int, List[Dict]]:
    """Find schools within the drive-time polygons of a target location"""
    try:
        nearby_schools = {}
        
        if not target_polygons:
            logger.warning(f"No polygons available for location {target_location_id}")
            return nearby_schools
        
        # Filter out the target location's own schools to avoid self-referencing
        other_schools = [school for school in all_schools if school['location_id'] != target_location_id]
        
        logger.info(f"Checking {len(other_schools)} schools against {len(target_polygons)} polygons for location {target_location_id}")
        
        for drive_time, polygon_data in target_polygons.items():
            polygon = polygon_data['polygon']
            schools_in_polygon = []
            
            for school in other_schools:
                try:
                    # Create point from school location
                    school_point = Point(school['longitude'], school['latitude'])
                    
                    # Check if school is within the polygon
                    if polygon.contains(school_point) or polygon.touches(school_point):
                        schools_in_polygon.append({
                            'school_uuid': school['school_uuid'],
                            'school_id': school['school_id'],
                            'external_school_id': school['external_school_id'],
                            'school_name': school['school_name'],
                            'location_id': school['location_id'],
                            'latitude': school['latitude'],
                            'longitude': school['longitude'],
                            'relationship_type': 'nearby'
                        })
                        
                except Exception as point_error:
                    logger.warning(f"Error checking school {school['school_uuid']} against polygon: {str(point_error)}")
                    continue
            
            nearby_schools[drive_time] = schools_in_polygon
            logger.info(f"Found {len(schools_in_polygon)} schools within {drive_time}-minute drive of location {target_location_id}")
        
        total_nearby = sum(len(schools) for schools in nearby_schools.values())
        logger.info(f"Total nearby schools found for location {target_location_id}: {total_nearby}")
        
        return nearby_schools
        
    except Exception as e:
        logger.error(f"Error finding nearby schools for location {target_location_id}: {str(e)}")
        return {}

def store_polygon_relationships(engine, location_id: int, data_year: int, nearby_schools: Dict[int, List[Dict]]) -> bool:
    """Store polygon relationships and nearby schools in the database"""
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                for drive_time, schools in nearby_schools.items():
                    # Insert or update polygon relationship
                    relationship_result = conn.execute(text("""
                        INSERT INTO school_polygon_relationships 
                        (location_id, drive_time, data_year, processed_at)
                        VALUES (:location_id, :drive_time, :data_year, CURRENT_TIMESTAMP)
                        ON CONFLICT (location_id, drive_time, data_year) 
                        DO UPDATE SET 
                            processed_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                    """), {
                        'location_id': location_id,
                        'drive_time': drive_time,
                        'data_year': data_year
                    })
                    
                    relationship_id = relationship_result.fetchone()[0]
                    
                    # Delete existing nearby schools for this relationship
                    conn.execute(text("""
                        DELETE FROM nearby_school_polygons 
                        WHERE polygon_relationship_id = :relationship_id
                    """), {'relationship_id': relationship_id})
                    
                    # Insert nearby schools
                    if schools:
                        for school in schools:
                            conn.execute(text("""
                                INSERT INTO nearby_school_polygons 
                                (polygon_relationship_id, school_uuid, relationship_type)
                                VALUES (:relationship_id, :school_uuid, :relationship_type)
                            """), {
                                'relationship_id': relationship_id,
                                'school_uuid': school['school_uuid'],
                                'relationship_type': school['relationship_type']
                            })
                    
                    logger.debug(f"Stored {len(schools)} nearby schools for drive_time {drive_time}")
                
                # Commit transaction
                trans.commit()
                
                total_schools = sum(len(schools) for schools in nearby_schools.values())
                logger.info(f"Successfully stored polygon relationships for location {location_id}: {len(nearby_schools)} polygons, {total_schools} total nearby schools")
                
                return True
                
            except Exception as store_error:
                trans.rollback()
                logger.error(f"Error storing data, transaction rolled back: {str(store_error)}")
                return False
                
    except Exception as e:
        logger.error(f"Error storing polygon relationships for location {location_id}: {str(e)}")
        return False

def process_nearby_schools_for_location(engine, location_id: int, data_year: int) -> bool:
    """Process nearby schools for a single location"""
    try:
        logger.info(f"Processing nearby schools for location {location_id}, data year {data_year}")
        
        # Get ESRI polygons for this location
        target_polygons = get_esri_polygons(engine, location_id)
        
        if not target_polygons:
            logger.warning(f"No ESRI polygons found for location {location_id}")
            return False
        
        # Get all schools with locations for this data year
        all_schools = get_all_schools_with_locations(engine, data_year)
        
        if not all_schools:
            logger.warning(f"No schools with locations found for data year {data_year}")
            return False
        
        # Find nearby schools within the polygons
        nearby_schools = find_nearby_schools(location_id, target_polygons, all_schools)
        
        # Store the results
        success = store_polygon_relationships(engine, location_id, data_year, nearby_schools)
        
        if success:
            total_nearby = sum(len(schools) for schools in nearby_schools.values())
            logger.info(f"Successfully processed location {location_id}: {len(nearby_schools)} polygons, {total_nearby} nearby schools")
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing nearby schools for location {location_id}: {str(e)}")
        return False

def validate_nearby_school_results(engine, location_id: int, data_year: int) -> Dict:
    """Validate the results of nearby school processing"""
    try:
        with engine.connect() as conn:
            # Check polygon relationships
            polygon_result = conn.execute(text("""
                SELECT COUNT(*) as polygon_count,
                       array_agg(DISTINCT drive_time ORDER BY drive_time) as drive_times
                FROM school_polygon_relationships
                WHERE location_id = :location_id AND data_year = :data_year
            """), {
                'location_id': location_id,
                'data_year': data_year
            }).fetchone()
            
            # Check nearby schools
            school_result = conn.execute(text("""
                SELECT COUNT(*) as school_count,
                       COUNT(DISTINCT nsp.school_uuid) as unique_schools
                FROM school_polygon_relationships spr
                JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                WHERE spr.location_id = :location_id AND spr.data_year = :data_year
            """), {
                'location_id': location_id,
                'data_year': data_year
            }).fetchone()
            
            polygon_count = polygon_result[0] if polygon_result else 0
            drive_times = polygon_result[1] if polygon_result else []
            school_count = school_result[0] if school_result else 0
            unique_schools = school_result[1] if school_result else 0
            
            # Determine validation status
            is_valid = polygon_count > 0
            error_messages = []
            
            if polygon_count == 0:
                error_messages.append("No polygon relationships found")
            
            # Check for expected drive times (5, 10, 15 minutes)
            expected_drive_times = {5, 10, 15}
            actual_drive_times = set(drive_times) if drive_times else set()
            missing_drive_times = expected_drive_times - actual_drive_times
            
            if missing_drive_times:
                error_messages.append(f"Missing drive times: {sorted(missing_drive_times)}")
            
            validation_result = {
                'is_valid': is_valid,
                'polygon_count': polygon_count,
                'school_count': school_count,
                'unique_schools': unique_schools,
                'drive_times': sorted(actual_drive_times),
                'missing_drive_times': sorted(missing_drive_times),
                'error': '; '.join(error_messages) if error_messages else None
            }
            
            if is_valid:
                logger.info(f"Validation passed for location {location_id}")
            else:
                logger.warning(f"Validation failed for location {location_id}: {validation_result['error']}")
            
            return validation_result
            
    except Exception as e:
        logger.error(f"Error validating results for location {location_id}: {str(e)}")
        return {
            'is_valid': False,
            'polygon_count': 0,
            'school_count': 0,
            'unique_schools': 0,
            'drive_times': [],
            'missing_drive_times': [5, 10, 15],
            'error': str(e)
        }

def get_processing_summary(engine, data_year: int = None) -> Dict:
    """Get summary of nearby schools processing status"""
    try:
        with engine.connect() as conn:
            params = {}
            where_clause = ""
            
            if data_year:
                where_clause = "WHERE spr.data_year = :data_year"
                params['data_year'] = data_year
            
            # Get overall summary
            summary_result = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as total_relationships,
                    COUNT(DISTINCT spr.location_id) as unique_locations,
                    COUNT(DISTINCT spr.data_year) as unique_years,
                    MIN(spr.processed_at) as earliest_processed,
                    MAX(spr.processed_at) as latest_processed,
                    COUNT(nsp.id) as total_nearby_schools,
                    COUNT(DISTINCT nsp.school_uuid) as unique_schools
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                {where_clause}
            """), params).fetchone()
            
            if not summary_result or summary_result[0] == 0:
                return {'total_relationships': 0}
            
            # Get drive time breakdown
            drive_time_result = conn.execute(text(f"""
                SELECT 
                    spr.drive_time,
                    COUNT(spr.id) as polygon_count,
                    COUNT(nsp.id) as nearby_school_count
                FROM school_polygon_relationships spr
                LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
                {where_clause}
                GROUP BY spr.drive_time
                ORDER BY spr.drive_time
            """), params).fetchall()
            
            drive_time_breakdown = []
            drive_times_processed = []
            
            for row in drive_time_result:
                drive_time_breakdown.append({
                    'drive_time': row[0],
                    'polygon_count': row[1],
                    'nearby_school_count': row[2]
                })
                drive_times_processed.append(row[0])
            
            summary = {
                'total_relationships': summary_result[0],
                'unique_locations': summary_result[1],
                'unique_years': summary_result[2],
                'earliest_processed': summary_result[3],
                'latest_processed': summary_result[4],
                'total_nearby_schools': summary_result[5],
                'unique_schools': summary_result[6],
                'drive_times_processed': sorted(drive_times_processed),
                'drive_time_breakdown': drive_time_breakdown,
                'data_year': data_year or 'All Years'
            }
            
            return summary
            
    except Exception as e:
        logger.error(f"Error getting processing summary: {str(e)}")
        return {'total_relationships': 0}

def cleanup_orphaned_records(engine) -> Dict:
    """Clean up orphaned records in nearby_school_polygons"""
    try:
        with engine.connect() as conn:
            # Find orphaned records
            orphan_result = conn.execute(text("""
                SELECT COUNT(*) as orphan_count
                FROM nearby_school_polygons nsp
                LEFT JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                WHERE spr.id IS NULL
            """)).fetchone()
            
            orphan_count = orphan_result[0] if orphan_result else 0
            
            if orphan_count > 0:
                # Delete orphaned records
                delete_result = conn.execute(text("""
                    DELETE FROM nearby_school_polygons 
                    WHERE id IN (
                        SELECT nsp.id
                        FROM nearby_school_polygons nsp
                        LEFT JOIN school_polygon_relationships spr ON nsp.polygon_relationship_id = spr.id
                        WHERE spr.id IS NULL
                    )
                """))
                
                conn.commit()
                deleted_count = delete_result.rowcount
                
                logger.info(f"Cleaned up {deleted_count} orphaned nearby school records")
                
                return {
                    'orphan_count': orphan_count,
                    'deleted_count': deleted_count,
                    'success': True
                }
            else:
                logger.info("No orphaned records found")
                return {
                    'orphan_count': 0,
                    'deleted_count': 0,
                    'success': True
                }
                
    except Exception as e:
        logger.error(f"Error cleaning up orphaned records: {str(e)}")
        return {
            'orphan_count': 0,
            'deleted_count': 0,
            'success': False,
            'error': str(e)
        } 