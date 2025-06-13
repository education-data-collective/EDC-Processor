"""
Metrics Processing

Processes district-level metrics for schools.
Can work with both schools and location points depending on available data.
"""

from flask import current_app
from models import School, DistrictMetrics, SchoolEnrollment, SchoolFRL
from ..utils import success_response, error_response, update_processing_status
from datetime import datetime
from models import db


async def process_metrics(payload):
    """Process district metrics calculation"""
    try:
        entity_id = payload.get('entity_id')
        entity_type = payload.get('entity_type', 'school')
        data_year = payload.get('data_year', datetime.now().year)
        
        current_app.logger.info(f"Processing metrics for {entity_type} {entity_id}")
        
        if entity_type == 'school':
            result = await process_school_metrics(entity_id, data_year)
        elif entity_type == 'location':
            # Location points don't typically generate district metrics
            current_app.logger.info(f"Metrics not applicable for location point {entity_id}")
            result = success_response({
                'entity_id': entity_id,
                'entity_type': entity_type,
                'action': 'skipped_not_applicable'
            }, "Metrics not applicable for location points")
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        return result
        
    except Exception as e:
        current_app.logger.error(f"Metrics processing error: {str(e)}")
        return error_response(str(e))


async def process_school_metrics(school_id, data_year):
    """Process district metrics for a school"""
    try:
        school = School.query.get(school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        
        # Calculate district metrics
        metrics_data = await calculate_district_metrics_for_school(school_id, data_year)
        
        if metrics_data:
            # Store metrics in database
            existing_metrics = DistrictMetrics.query.filter_by(
                school_id=school_id,
                data_year=data_year
            ).first()
            
            if existing_metrics:
                # Update existing metrics
                for key, value in metrics_data.items():
                    if hasattr(existing_metrics, key):
                        setattr(existing_metrics, key, value)
                existing_metrics.updated_at = datetime.utcnow()
            else:
                # Create new metrics record
                new_metrics = DistrictMetrics(
                    school_id=school_id,
                    data_year=data_year,
                    created_at=datetime.utcnow(),
                    **metrics_data
                )
                db.session.add(new_metrics)
            
            db.session.commit()
            
            result_data = {
                'school_id': school_id,
                'data_year': data_year,
                'action': 'calculated',
                'metrics': metrics_data
            }
        else:
            result_data = {
                'school_id': school_id,
                'data_year': data_year,
                'action': 'skipped_insufficient_data',
                'message': 'Insufficient data for district metrics calculation'
            }
        
        # Update processing status
        update_processing_status(school_id, 'school', 'metrics', 'completed', data_year)
        
        return success_response(result_data, "Metrics processing completed")
        
    except Exception as e:
        current_app.logger.error(f"School metrics processing error: {str(e)}")
        
        # Update processing status on failure
        try:
            update_processing_status(school_id, 'school', 'metrics', 'failed', data_year)
        except:
            pass
        
        raise e


async def calculate_district_metrics_for_school(school_id, data_year):
    """
    Calculate district metrics for a school
    
    This function adapts your existing district metrics calculation logic.
    You'll need to import/adapt your existing calculate_district_metrics function here.
    """
    try:
        current_app.logger.info(f"Calculating district metrics for school {school_id}")
        
        # Placeholder - replace with your actual district metrics logic
        # from app.services.district_metrics.calculator import calculate_district_metrics
        # return calculate_district_metrics(school_id, data_year)
        
        # Get enrollment data
        enrollment = SchoolEnrollment.query.filter_by(
            school_id=school_id,
            data_year=data_year
        ).first()
        
        if not enrollment:
            current_app.logger.warning(f"No enrollment data for metrics calculation: school {school_id}, year {data_year}")
            return None
        
        # Get FRL data if available
        frl_data = SchoolFRL.query.filter_by(
            school_id=school_id,
            data_year=data_year
        ).first()
        
        # Calculate basic metrics
        metrics = {
            'total_enrollment': enrollment.total_enrollment,
            'total_schools': 1,  # This school
            'average_school_size': enrollment.total_enrollment
        }
        
        # Add FRL metrics if available
        if frl_data and hasattr(frl_data, 'frl_count') and frl_data.frl_count is not None:
            metrics['total_frl_count'] = frl_data.frl_count
            if enrollment.total_enrollment > 0:
                metrics['frl_percentage'] = (frl_data.frl_count / enrollment.total_enrollment) * 100
            else:
                metrics['frl_percentage'] = 0.0
        else:
            metrics['total_frl_count'] = None
            metrics['frl_percentage'] = None
        
        current_app.logger.info(f"Calculated metrics for school {school_id}: {metrics}")
        return metrics
        
    except Exception as e:
        current_app.logger.error(f"Metrics calculation error: {str(e)}")
        return None


def get_metrics_summary(school_id, data_year=None):
    """Get metrics summary for a school"""
    try:
        school = School.query.get(school_id)
        if not school:
            return None
        
        query = DistrictMetrics.query.filter_by(school_id=school_id)
        if data_year:
            query = query.filter_by(data_year=data_year)
        
        metrics = query.order_by(DistrictMetrics.data_year.desc()).all()
        
        if not metrics:
            return {
                'school_id': school_id,
                'has_metrics': False,
                'years_available': []
            }
        
        summary = {
            'school_id': school_id,
            'has_metrics': True,
            'years_available': [],
            'latest_year': None,
            'latest_metrics': None
        }
        
        for metric in metrics:
            metric_data = {
                'data_year': metric.data_year,
                'total_enrollment': metric.total_enrollment,
                'total_schools': metric.total_schools,
                'average_school_size': metric.average_school_size,
                'total_frl_count': metric.total_frl_count,
                'frl_percentage': metric.frl_percentage
            }
            summary['years_available'].append(metric_data)
        
        if metrics:
            latest = metrics[0]  # First one due to desc order
            summary['latest_year'] = latest.data_year
            summary['latest_metrics'] = {
                'total_enrollment': latest.total_enrollment,
                'total_schools': latest.total_schools,
                'average_school_size': latest.average_school_size,
                'total_frl_count': latest.total_frl_count,
                'frl_percentage': latest.frl_percentage
            }
        
        return summary
        
    except Exception as e:
        current_app.logger.error(f"Error getting metrics summary: {str(e)}")
        return None


def validate_metrics_data(metrics_data):
    """Validate metrics data structure"""
    try:
        # Define expected numeric fields
        numeric_fields = [
            'total_enrollment', 'total_schools', 'average_school_size', 
            'total_frl_count', 'frl_percentage'
        ]
        
        for field in numeric_fields:
            if field in metrics_data and metrics_data[field] is not None:
                if not isinstance(metrics_data[field], (int, float)):
                    return False, f"{field} must be a number"
                
                # Specific validations
                if field in ['total_enrollment', 'total_schools', 'total_frl_count']:
                    if metrics_data[field] < 0:
                        return False, f"{field} cannot be negative"
                
                if field == 'frl_percentage':
                    if not (0 <= metrics_data[field] <= 100):
                        return False, "FRL percentage must be between 0 and 100"
        
        return True, None
        
    except Exception as e:
        return False, str(e) 