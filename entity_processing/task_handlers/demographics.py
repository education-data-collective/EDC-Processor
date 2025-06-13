"""
Demographics Processing

Processes ESRI demographic data for location points.
Works with both school locations and standalone location points.
"""

from flask import current_app
from models import LocationPoint, EsriDemographicData, db
from ..utils import success_response, error_response, update_processing_status
from datetime import datetime


async def process_demographics(payload):
    """Process ESRI demographic data for a location point"""
    try:
        location_id = payload.get('location_id')
        coordinates = payload.get('coordinates')
        entity_id = payload.get('entity_id')
        entity_type = payload.get('entity_type', 'location')
        
        current_app.logger.info(f"Processing demographics for location {location_id}")
        
        if not location_id or not coordinates:
            raise ValueError("Missing location_id or coordinates")
        
        # Verify location exists
        location = LocationPoint.query.get(location_id)
        if not location:
            raise ValueError(f"Location point not found: {location_id}")
        
        # Verify coordinates
        latitude = coordinates.get('latitude')
        longitude = coordinates.get('longitude')
        if not latitude or not longitude:
            raise ValueError("Invalid coordinates provided")
        
        current_app.logger.info(f"Fetching ESRI data for coordinates: {latitude}, {longitude}")
        
        # Import ESRI fetch functionality (adapted from original)
        # Note: You'll need to adapt your existing fetch_esri_data function
        esri_data = await fetch_esri_data_for_location(latitude, longitude)
        
        if not esri_data:
            raise ValueError("Failed to fetch ESRI data")
        
        # Store demographic data for each drive time
        stored_entries = 0
        for drive_time, data in esri_data.items():
            try:
                # Check for existing record
                existing = EsriDemographicData.query.filter_by(
                    location_id=location_id,
                    drive_time=drive_time
                ).first()
                
                if existing:
                    # Update existing record
                    for key, value in data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    existing.timestamp = datetime.utcnow()
                else:
                    # Create new record
                    demographic_data = EsriDemographicData(
                        location_id=location_id,
                        drive_time=drive_time,
                        timestamp=datetime.utcnow(),
                        **{k: v for k, v in data.items() if hasattr(EsriDemographicData, k)}
                    )
                    db.session.add(demographic_data)
                
                stored_entries += 1
                
            except Exception as e:
                current_app.logger.error(f"Error storing drive time {drive_time} data: {str(e)}")
                continue
        
        # Commit the transaction
        db.session.commit()
        
        # Update processing status if this is for a school
        if entity_type == 'school':
            update_processing_status(entity_id, entity_type, 'demographics', 'completed')
        
        current_app.logger.info(f"Successfully processed {stored_entries} demographic entries for location {location_id}")
        
        return success_response({
            'location_id': location_id,
            'stored_entries': stored_entries,
            'coordinates': coordinates
        }, f"Demographics processed for {stored_entries} drive time zones")
        
    except Exception as e:
        current_app.logger.error(f"Demographics processing error: {str(e)}")
        
        # Update processing status on failure
        if payload.get('entity_type') == 'school':
            try:
                update_processing_status(
                    payload.get('entity_id'), 
                    payload.get('entity_type'), 
                    'demographics', 
                    'failed'
                )
            except:
                pass  # Don't fail the main error handling
        
        return error_response(str(e))


async def fetch_esri_data_for_location(latitude, longitude):
    """
    Fetch ESRI data for coordinates
    
    This function adapts your existing ESRI fetch logic to work with the new system.
    You'll need to import/adapt your existing fetch_esri_data function here.
    """
    try:
        # Placeholder - replace with your actual ESRI fetch logic
        # from app.esri.fetch import fetch_esri_data
        # return fetch_esri_data(latitude, longitude)
        
        current_app.logger.info(f"Fetching ESRI data for {latitude}, {longitude}")
        
        # For now, return placeholder data structure
        # Replace this with your actual ESRI integration
        return {
            5: {  # 5-minute drive time
                'total_population': 1000,
                'household_count': 400,
                'median_household_income': 65000,
                'age_5_to_9': 80,
                'age_10_to_14': 75,
                'has_data': 1
            },
            10: {  # 10-minute drive time
                'total_population': 2500,
                'household_count': 950,
                'median_household_income': 68000,
                'age_5_to_9': 200,
                'age_10_to_14': 185,
                'has_data': 1
            }
        }
        
    except Exception as e:
        current_app.logger.error(f"ESRI fetch error: {str(e)}")
        return None


def get_demographic_summary(location_id):
    """Get summary of demographic data for a location"""
    try:
        demographics = EsriDemographicData.query.filter_by(
            location_id=location_id
        ).all()
        
        if not demographics:
            return None
        
        summary = {
            'location_id': location_id,
            'drive_times': [],
            'last_updated': None,
            'total_entries': len(demographics)
        }
        
        for demo in demographics:
            summary['drive_times'].append({
                'drive_time': demo.drive_time,
                'population': demo.total_population,
                'households': demo.household_count,
                'median_income': demo.median_household_income
            })
            
            # Track most recent update
            if not summary['last_updated'] or demo.timestamp > summary['last_updated']:
                summary['last_updated'] = demo.timestamp
        
        return summary
        
    except Exception as e:
        current_app.logger.error(f"Error getting demographic summary: {str(e)}")
        return None 