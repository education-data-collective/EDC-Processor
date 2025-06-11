"""
Projections Processing

Processes enrollment projections for schools with enrollment data.
Not applicable to standalone location points or schools without enrollment.
"""

from flask import current_app
from models import School, SchoolEnrollment, SchoolProjection
from ..utils import success_response, error_response, update_processing_status
from datetime import datetime
from app import db


async def process_projections(payload):
    """Process enrollment projections for a school"""
    try:
        school_id = payload.get('school_id')
        data_year = payload.get('data_year', datetime.now().year)
        
        current_app.logger.info(f"Processing projections for school {school_id}")
        
        if not school_id:
            raise ValueError("Missing school_id")
        
        school = School.query.get(school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        
        # Check if school has enrollment data
        enrollment_data = SchoolEnrollment.query.filter_by(
            school_id=school_id
        ).order_by(SchoolEnrollment.data_year.desc()).all()
        
        if not enrollment_data:
            current_app.logger.info(f"No enrollment data available for school {school_id}, skipping projections")
            result_data = {
                'school_id': school_id,
                'data_year': data_year,
                'action': 'skipped_no_enrollment',
                'message': 'Projections require historical enrollment data'
            }
        else:
            # Generate projections
            projections_result = await generate_school_projections(school_id, enrollment_data, data_year)
            
            if projections_result:
                result_data = {
                    'school_id': school_id,
                    'data_year': data_year,
                    'action': 'generated',
                    'projections': projections_result
                }
            else:
                raise ValueError("Failed to generate projections")
        
        # Update processing status
        update_processing_status(school_id, 'school', 'projections', 'completed', data_year)
        
        return success_response(result_data, "Projections processing completed")
        
    except Exception as e:
        current_app.logger.error(f"Projections processing error: {str(e)}")
        
        # Update processing status on failure
        if payload.get('school_id'):
            try:
                update_processing_status(
                    payload.get('school_id'), 
                    'school', 
                    'projections', 
                    'failed',
                    payload.get('data_year', datetime.now().year)
                )
            except:
                pass
        
        return error_response(str(e))


async def generate_school_projections(school_id, enrollment_data, target_year):
    """
    Generate enrollment projections for a school
    
    This function adapts your existing projections logic.
    You'll need to import/adapt your existing generate_and_update_projections function here.
    """
    try:
        current_app.logger.info(f"Generating projections for school {school_id}")
        
        # Prepare historical enrollment data for projections algorithm
        historical_data = {}
        for enrollment in enrollment_data:
            year = enrollment.data_year
            historical_data[year] = {
                'total': enrollment.total_enrollment
            }
            
            # Add grade-level data if available
            if hasattr(enrollment, 'grade_pk') and enrollment.grade_pk:
                historical_data[year]['pk'] = enrollment.grade_pk
            if hasattr(enrollment, 'grade_k') and enrollment.grade_k:
                historical_data[year]['k'] = enrollment.grade_k
            # Add other grades as needed...
        
        if len(historical_data) < 2:
            current_app.logger.warning(f"Insufficient historical data for projections: {len(historical_data)} years")
            return None
        
        # Placeholder projection logic - replace with your actual algorithm
        # from enrollment_projections.main import generate_and_update_projections
        # projections = generate_and_update_projections(school_id, historical_data)
        
        # For now, simple projection based on average growth
        years = sorted(historical_data.keys())
        recent_years = years[-3:] if len(years) >= 3 else years
        
        total_enrollments = [historical_data[year]['total'] for year in recent_years if historical_data[year]['total']]
        
        if len(total_enrollments) >= 2:
            # Simple linear projection
            growth_rates = []
            for i in range(1, len(total_enrollments)):
                if total_enrollments[i-1] > 0:
                    growth_rate = (total_enrollments[i] - total_enrollments[i-1]) / total_enrollments[i-1]
                    growth_rates.append(growth_rate)
            
            avg_growth = sum(growth_rates) / len(growth_rates) if growth_rates else 0
            latest_enrollment = total_enrollments[-1]
            
            # Project forward
            projection_years = range(target_year, target_year + 6)  # 5-year projection
            projections = {}
            
            for i, proj_year in enumerate(projection_years):
                projected_total = int(latest_enrollment * ((1 + avg_growth) ** (i + 1)))
                projections[proj_year] = {
                    'total_enrollment': max(0, projected_total),
                    'method': 'linear_growth',
                    'confidence': 'medium' if i <= 2 else 'low'
                }
        else:
            # Fallback: assume flat enrollment
            latest_enrollment = total_enrollments[-1] if total_enrollments else 100
            projection_years = range(target_year, target_year + 6)
            projections = {
                year: {
                    'total_enrollment': latest_enrollment,
                    'method': 'flat_projection',
                    'confidence': 'low'
                }
                for year in projection_years
            }
        
        # Store projections in database
        for proj_year, proj_data in projections.items():
            existing_projection = SchoolProjection.query.filter_by(
                school_id=school_id,
                projection_year=proj_year
            ).first()
            
            if existing_projection:
                existing_projection.projected_enrollment = proj_data['total_enrollment']
                existing_projection.projection_method = proj_data['method']
                existing_projection.updated_at = datetime.utcnow()
            else:
                new_projection = SchoolProjection(
                    school_id=school_id,
                    projection_year=proj_year,
                    projected_enrollment=proj_data['total_enrollment'],
                    projection_method=proj_data['method'],
                    created_at=datetime.utcnow()
                )
                db.session.add(new_projection)
        
        db.session.commit()
        
        current_app.logger.info(f"Generated projections for {len(projections)} years")
        return projections
        
    except Exception as e:
        current_app.logger.error(f"Projection generation error: {str(e)}")
        return None


def get_projections_summary(school_id):
    """Get projections summary for a school"""
    try:
        school = School.query.get(school_id)
        if not school:
            return None
        
        projections = SchoolProjection.query.filter_by(
            school_id=school_id
        ).order_by(SchoolProjection.projection_year).all()
        
        if not projections:
            return {
                'school_id': school_id,
                'has_projections': False,
                'projection_years': []
            }
        
        summary = {
            'school_id': school_id,
            'has_projections': True,
            'projection_years': [],
            'latest_update': None
        }
        
        for projection in projections:
            summary['projection_years'].append({
                'year': projection.projection_year,
                'projected_enrollment': projection.projected_enrollment,
                'method': projection.projection_method if hasattr(projection, 'projection_method') else None
            })
            
            # Track most recent update
            update_time = getattr(projection, 'updated_at', None) or getattr(projection, 'created_at', None)
            if update_time and (not summary['latest_update'] or update_time > summary['latest_update']):
                summary['latest_update'] = update_time
        
        return summary
        
    except Exception as e:
        current_app.logger.error(f"Error getting projections summary: {str(e)}")
        return None 