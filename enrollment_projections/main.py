from typing import Dict, Any, List
from flask import current_app
from .data_fetcher import fetch_historical_data, fetch_school_info
from .user_data_processor import process_user_data
from .survival_rates import calculate_survival_rates, calculate_forecast_survival_rates
from .projections import calculate_entry_grade_estimates, generate_projections
from .data_structures import SchoolData
from .utils import GRADE_MAP, generate_forecast_years, get_most_recent_year
import traceback
import json
from datetime import datetime

def convert_firestore_timestamp(obj):
    if hasattr(obj, 'seconds') and hasattr(obj, 'nanoseconds'):
        # This is likely a Firestore Timestamp object
        return datetime.fromtimestamp(obj.seconds + obj.nanoseconds / 1e9).isoformat()
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def serialize_for_log(obj):
    if isinstance(obj, dict):
        return {k: serialize_for_log(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_for_log(v) for v in obj]
    return convert_firestore_timestamp(obj)

def generate_and_update_projections(ncessch: str, user_data: Dict[str, Any] = None) -> Dict[str, Any]:
    try:
        current_app.logger.info(f"Starting generate_and_update_projections for ncessch: {ncessch}")

        # Log the raw user data first, if it exists
        if user_data is not None:
            # Convert Firestore timestamps before logging
            serializable_user_data = serialize_for_log(user_data)
        else:
            current_app.logger.info("No user data provided, using historical data only.")

        # Fetch historical data
        with current_app.app_context():
            historical_data = fetch_historical_data(ncessch)

        # Fetch school info
        with current_app.app_context():
            school_info = fetch_school_info(ncessch)

        if not historical_data or not school_info:
            current_app.logger.error(f"No data found for school: {ncessch}")
            return {'error': 'No data found for the given school'}

        # Combine historical data with school info
        school_data: SchoolData = {
            'id': school_info['id'],
            'ncessch': school_info['ncessch'],
            'school_name': school_info['school_name'],
            'enrollment': {}
        }

        # Process historical data
        for item in historical_data:
            year = item['school_year']
            grade = item['grade']
            enrollment = item['total_enrollment']
            if year and grade and enrollment is not None:
                if year not in school_data['enrollment']:
                    school_data['enrollment'][year] = {}
                school_data['enrollment'][year][grade] = enrollment

        # Process user data if it exists
        if user_data is not None:
            enrollment_data = user_data.get('enrollmentData', {})
            for year, year_data in enrollment_data.items():
                if year not in school_data['enrollment']:
                    school_data['enrollment'][year] = {}
                grades = year_data.get('grades', {})
                
                # Track discontinued grades to remove from historical data
                discontinued_grades = {
                    grade for grade, enrollment in grades.items() 
                    if enrollment == -1
                }
                
                # Update enrollments
                for grade, enrollment in grades.items():
                    school_data['enrollment'][year][grade] = enrollment
            
            # If grade is discontinued, remove from future projections
            if enrollment == -1:
                school_data['discontinued_grades'] = school_data.get('discontinued_grades', set())
                school_data['discontinued_grades'].add(grade)

            # Update school name if provided
            if 'schoolName' in user_data:
                school_data['school_name'] = user_data['schoolName']

        # Determine the most recent year and generate forecast years
        most_recent_year = get_most_recent_year(school_data['enrollment'])
        forecast_years = generate_forecast_years(most_recent_year)

        # Calculate survival rates
        school_data = calculate_survival_rates(school_data, GRADE_MAP)

        # Calculate forecast survival rates
        school_data = calculate_forecast_survival_rates(school_data)

        # Calculate entry grade estimates
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)

        # Generate projections
        school_data = generate_projections(school_data, GRADE_MAP, forecast_years)

        if 'projections' not in school_data:
            return {'error': 'Unable to generate projections for the given school'}

        # Prepare result
        result = {
            'school_info': {
                'id': school_data['id'],
                'ncessch': school_data['ncessch'],
                'school_name': school_data['school_name']
            },
            'actual_enrollment': school_data['enrollment'],
            'projections': school_data['projections'],
            'based_on_user_data': user_data is not None,
            'survivalRates': school_data.get('survivalRates'),
            'forecastSurvivalRates': school_data.get('forecastSurvivalRates'),
            'entryGrade': school_data.get('entryGrade'),
            'entryGradeEstimates': school_data.get('entryGradeEstimates')
        }

        current_app.logger.info(f"Projections generated successfully for school: {ncessch}")

        return result
    except Exception as e:
        current_app.logger.error(f"Error in generate_and_update_projections: {str(e)}")
        current_app.logger.error(f"Traceback: {traceback.format_exc()}")
        return {'error': str(e)}