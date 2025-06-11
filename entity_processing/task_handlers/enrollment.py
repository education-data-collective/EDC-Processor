"""
Enrollment Processing

Processes enrollment data for schools.
Not applicable to standalone location points.
"""

from flask import current_app
from models import School, SchoolEnrollment
from ..utils import success_response, error_response, update_processing_status
from datetime import datetime
from app import db


async def process_enrollment_data(payload):
    """Process enrollment data for a school"""
    try:
        school_id = payload.get('school_id')
        data_year = payload.get('data_year', datetime.now().year)
        
        current_app.logger.info(f"Processing enrollment data for school {school_id}")
        
        if not school_id:
            raise ValueError("Missing school_id")
        
        school = School.query.get(school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        
        # Check if enrollment data exists for this year
        existing_enrollment = SchoolEnrollment.query.filter_by(
            school_id=school_id,
            data_year=data_year
        ).first()
        
        if existing_enrollment:
            current_app.logger.info(f"Enrollment data already exists for school {school_id}, year {data_year}")
            result_data = {
                'school_id': school_id,
                'data_year': data_year,
                'total_enrollment': existing_enrollment.total_enrollment,
                'action': 'validated_existing'
            }
        else:
            # For now, we'll just validate that the school exists
            # In a real implementation, you might import enrollment data from external sources
            current_app.logger.info(f"No enrollment data found for school {school_id}, year {data_year}")
            result_data = {
                'school_id': school_id,
                'data_year': data_year,
                'total_enrollment': None,
                'action': 'no_data_available'
            }
        
        # Update processing status
        update_processing_status(school_id, 'school', 'enrollment', 'completed', data_year)
        
        return success_response(result_data, "Enrollment data processing completed")
        
    except Exception as e:
        current_app.logger.error(f"Enrollment processing error: {str(e)}")
        
        # Update processing status on failure
        if payload.get('school_id'):
            try:
                update_processing_status(
                    payload.get('school_id'), 
                    'school', 
                    'enrollment', 
                    'failed',
                    payload.get('data_year', datetime.now().year)
                )
            except:
                pass
        
        return error_response(str(e))


def get_enrollment_summary(school_id, data_year=None):
    """Get enrollment summary for a school"""
    try:
        school = School.query.get(school_id)
        if not school:
            return None
        
        query = SchoolEnrollment.query.filter_by(school_id=school_id)
        if data_year:
            query = query.filter_by(data_year=data_year)
        
        enrollments = query.order_by(SchoolEnrollment.data_year.desc()).all()
        
        if not enrollments:
            return {
                'school_id': school_id,
                'has_enrollment': False,
                'years_available': []
            }
        
        summary = {
            'school_id': school_id,
            'has_enrollment': True,
            'years_available': [],
            'latest_year': None,
            'latest_total': None
        }
        
        for enrollment in enrollments:
            summary['years_available'].append({
                'data_year': enrollment.data_year,
                'total_enrollment': enrollment.total_enrollment,
                'grade_distribution': enrollment.grade_distribution if hasattr(enrollment, 'grade_distribution') else None
            })
        
        if enrollments:
            latest = enrollments[0]  # First one due to desc order
            summary['latest_year'] = latest.data_year
            summary['latest_total'] = latest.total_enrollment
        
        return summary
        
    except Exception as e:
        current_app.logger.error(f"Error getting enrollment summary: {str(e)}")
        return None


def validate_enrollment_data(enrollment_data):
    """Validate enrollment data structure"""
    try:
        required_fields = ['school_id', 'data_year', 'total_enrollment']
        
        for field in required_fields:
            if field not in enrollment_data:
                return False, f"Missing required field: {field}"
        
        # Validate data types
        if not isinstance(enrollment_data['total_enrollment'], (int, float)):
            return False, "Total enrollment must be a number"
        
        if enrollment_data['total_enrollment'] < 0:
            return False, "Total enrollment cannot be negative"
        
        # Validate year
        current_year = datetime.now().year
        if not (1990 <= enrollment_data['data_year'] <= current_year + 1):
            return False, f"Data year must be between 1990 and {current_year + 1}"
        
        return True, None
        
    except Exception as e:
        return False, str(e) 