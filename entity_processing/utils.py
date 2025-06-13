"""
Entity Processing Utilities

Helper functions for the new entity processing system.
"""

from models import School, LocationPoint, SchoolLocation, SchoolEnrollment


def get_entity_info(entity_id, entity_type):
    """Get entity information based on type"""
    if entity_type == 'school':
        school = School.query.get(entity_id)
        if not school:
            return None
        
        # Get current location
        current_location = None
        for location in school.locations:
            if location.is_current:
                current_location = location
                break
        
        return {
            'entity': school,
            'location': current_location,
            'has_location': bool(current_location and current_location.location_point.latitude),
            'has_enrollment': bool(school.enrollments),
            'name': school.name
        }
    
    elif entity_type == 'location':
        location = LocationPoint.query.get(entity_id)
        if not location:
            return None
        
        return {
            'entity': location,
            'location': location,
            'has_location': bool(location.latitude),
            'has_enrollment': False,  # Location points don't have enrollment
            'name': f"Location {location.id}"
        }
    
    return None


def determine_applicable_stages(entity_info, entity_type):
    """Determine which processing stages apply to this entity"""
    stages = []
    
    # All entities can have demographics if they have coordinates
    if entity_info['has_location']:
        stages.append('demographics')
    
    # Schools can have additional stages
    if entity_type == 'school':
        if entity_info['has_location']:
            stages.append('location')  # For location validation/updates
        
        if entity_info['has_enrollment']:
            stages.append('enrollment')
            stages.append('projections')
            stages.append('metrics')
    
    return stages


def update_processing_status(entity_id, entity_type, stage, status, data_year=None):
    """Update processing status in database"""
    from datetime import datetime
    from models import db
    from models import ProcessingStatus
    
    if entity_type != 'school':
        return  # Only schools have processing status in DB
    
    data_year = data_year or datetime.now().year
    
    # Find or create processing status
    proc_status = ProcessingStatus.query.filter_by(
        school_id=entity_id,
        data_year=data_year
    ).first()
    
    if not proc_status:
        proc_status = ProcessingStatus(
            school_id=entity_id,
            data_year=data_year
        )
        db.session.add(proc_status)
    
    # Update specific stage flags
    stage_mapping = {
        'location': 'location_processed',
        'demographics': 'demographics_processed',
        'enrollment': 'enrollment_processed',
        'projections': 'projections_processed',
        'metrics': 'district_metrics_processed'
    }
    
    if stage in stage_mapping:
        setattr(proc_status, stage_mapping[stage], status == 'completed')
        proc_status.last_processed_at = datetime.utcnow()
    
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        raise e


def validate_entity(entity_id, entity_type):
    """Validate entity exists and has required data"""
    entity_info = get_entity_info(entity_id, entity_type)
    
    if not entity_info:
        return {
            'valid': False,
            'error': f'{entity_type.title()} not found'
        }
    
    # Check basic requirements
    if not entity_info['has_location']:
        return {
            'valid': False,
            'error': 'No valid coordinates available'
        }
    
    return {
        'valid': True,
        'entity_info': entity_info,
        'applicable_stages': determine_applicable_stages(entity_info, entity_type)
    }


def get_processing_payload(entity_info, entity_type, stage):
    """Create payload for specific processing stage"""
    base_payload = {
        'entity_id': entity_info['entity'].id,
        'entity_type': entity_type,
        'entity_name': entity_info['name']
    }
    
    if stage == 'demographics':
        if entity_type == 'school':
            location = entity_info['location']
            coordinates = {
                'latitude': location.location_point.latitude,
                'longitude': location.location_point.longitude
            }
            base_payload.update({
                'location_id': location.location_point.id,
                'coordinates': coordinates
            })
        else:  # location point
            location_point = entity_info['entity']
            base_payload.update({
                'location_id': location_point.id,
                'coordinates': {
                    'latitude': location_point.latitude,
                    'longitude': location_point.longitude
                }
            })
    
    elif stage in ['enrollment', 'projections', 'metrics']:
        if entity_type == 'school':
            base_payload.update({
                'school_id': entity_info['entity'].id,
                'school': entity_info['entity']
            })
    
    return base_payload


def success_response(data=None, message=None):
    """Create standardized success response"""
    response = {'status': 'success'}
    if data is not None:
        response['data'] = data
    if message:
        response['message'] = message
    return response


def error_response(error, details=None):
    """Create standardized error response"""
    response = {
        'status': 'error',
        'error': str(error)
    }
    if details:
        response['details'] = details
    return response 