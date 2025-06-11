"""
Location Processing

Handles location data processing including geocoding and validation.
Works with both school locations and standalone location points.
"""

from flask import current_app
from models import School, LocationPoint, SchoolLocation
from ..utils import success_response, error_response, update_processing_status
from datetime import datetime
from app import db


async def process_location_data(payload):
    """Process location data for validation and geocoding"""
    try:
        entity_id = payload.get('entity_id')
        entity_type = payload.get('entity_type', 'school')
        data_year = payload.get('data_year', datetime.now().year)
        
        current_app.logger.info(f"Processing location data for {entity_type} {entity_id}")
        
        if entity_type == 'school':
            return await process_school_location(entity_id, data_year)
        elif entity_type == 'location':
            return await process_location_point(entity_id)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")
            
    except Exception as e:
        current_app.logger.error(f"Location processing error: {str(e)}")
        return error_response(str(e))


async def process_school_location(school_id, data_year):
    """Process location data for a school"""
    try:
        school = School.query.get(school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        
        # Get current location
        current_location = None
        for location in school.locations:
            if location.is_current and location.data_year == data_year:
                current_location = location
                break
        
        if not current_location:
            raise ValueError(f"No current location found for school {school_id}")
        
        location_point = current_location.location_point
        
        # Validate coordinates
        if not location_point.latitude or not location_point.longitude:
            # Attempt geocoding if address available
            if location_point.address:
                coordinates = await geocode_address(
                    location_point.address,
                    location_point.city,
                    location_point.state,
                    location_point.zip_code
                )
                
                if coordinates:
                    location_point.latitude = coordinates['latitude']
                    location_point.longitude = coordinates['longitude']
                    db.session.commit()
                    
                    current_app.logger.info(f"Successfully geocoded school {school_id}")
                else:
                    raise ValueError("Geocoding failed - no coordinates returned")
            else:
                raise ValueError("No address available for geocoding")
        
        # Update processing status
        update_processing_status(school_id, 'school', 'location', 'completed', data_year)
        
        return success_response({
            'school_id': school_id,
            'location_id': location_point.id,
            'coordinates': {
                'latitude': location_point.latitude,
                'longitude': location_point.longitude
            },
            'address': {
                'street': location_point.address,
                'city': location_point.city,
                'state': location_point.state,
                'zip': location_point.zip_code
            }
        }, "School location validated successfully")
        
    except Exception as e:
        current_app.logger.error(f"School location processing error: {str(e)}")
        
        # Update processing status on failure
        try:
            update_processing_status(school_id, 'school', 'location', 'failed', data_year)
        except:
            pass
        
        raise e


async def process_location_point(location_id):
    """Process a standalone location point"""
    try:
        location = LocationPoint.query.get(location_id)
        if not location:
            raise ValueError(f"Location point not found: {location_id}")
        
        # Validate coordinates
        if not location.latitude or not location.longitude:
            # Attempt geocoding if address available
            if location.address:
                coordinates = await geocode_address(
                    location.address,
                    location.city,
                    location.state,
                    location.zip_code
                )
                
                if coordinates:
                    location.latitude = coordinates['latitude']
                    location.longitude = coordinates['longitude']
                    db.session.commit()
                    
                    current_app.logger.info(f"Successfully geocoded location {location_id}")
                else:
                    raise ValueError("Geocoding failed - no coordinates returned")
            else:
                raise ValueError("No address available for geocoding")
        
        return success_response({
            'location_id': location_id,
            'coordinates': {
                'latitude': location.latitude,
                'longitude': location.longitude
            },
            'address': {
                'street': location.address,
                'city': location.city,
                'state': location.state,
                'zip': location.zip_code
            }
        }, "Location point validated successfully")
        
    except Exception as e:
        current_app.logger.error(f"Location point processing error: {str(e)}")
        raise e


async def geocode_address(street, city, state, zip_code):
    """
    Geocode an address to coordinates
    
    This function adapts your existing geocoding logic.
    You'll need to import/adapt your existing GeocodingService here.
    """
    try:
        # Placeholder - replace with your actual geocoding logic
        # from app.utils.geocoding import GeocodingService
        # geocoding_service = GeocodingService()
        # return geocoding_service.geocode_address(street, city, state, zip_code)
        
        current_app.logger.info(f"Geocoding address: {street}, {city}, {state} {zip_code}")
        
        # For now, return placeholder coordinates
        # Replace this with your actual geocoding integration
        if street and city and state:
            return {
                'latitude': 40.7128,  # Placeholder coordinates (NYC)
                'longitude': -74.0060
            }
        
        return None
        
    except Exception as e:
        current_app.logger.error(f"Geocoding error: {str(e)}")
        return None


def validate_coordinates(latitude, longitude):
    """Validate coordinate values"""
    try:
        lat = float(latitude)
        lon = float(longitude)
        
        # Basic coordinate validation
        if not (-90 <= lat <= 90):
            return False, "Latitude must be between -90 and 90"
        
        if not (-180 <= lon <= 180):
            return False, "Longitude must be between -180 and 180"
        
        # Check for obviously invalid coordinates (0,0)
        if lat == 0 and lon == 0:
            return False, "Coordinates appear to be null island (0,0)"
        
        return True, None
        
    except (ValueError, TypeError):
        return False, "Invalid coordinate format"


def get_location_summary(entity_id, entity_type):
    """Get location summary for an entity"""
    try:
        if entity_type == 'school':
            school = School.query.get(entity_id)
            if not school:
                return None
            
            current_location = None
            for location in school.locations:
                if location.is_current:
                    current_location = location
                    break
            
            if not current_location:
                return {'has_location': False}
            
            location_point = current_location.location_point
            
        elif entity_type == 'location':
            location_point = LocationPoint.query.get(entity_id)
            if not location_point:
                return None
        else:
            return None
        
        has_coordinates = bool(location_point.latitude and location_point.longitude)
        
        summary = {
            'location_id': location_point.id,
            'has_location': True,
            'has_coordinates': has_coordinates,
            'address': {
                'street': location_point.address,
                'city': location_point.city,
                'state': location_point.state,
                'zip': location_point.zip_code
            }
        }
        
        if has_coordinates:
            summary['coordinates'] = {
                'latitude': location_point.latitude,
                'longitude': location_point.longitude
            }
        
        return summary
        
    except Exception as e:
        current_app.logger.error(f"Error getting location summary: {str(e)}")
        return None 