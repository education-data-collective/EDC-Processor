from .data_structures import SchoolData
from typing import Dict, List
import statistics as stats
from .utils import generate_forecast_years, get_most_recent_year

def calculate_entry_grade_estimates(school_data: SchoolData, grade_map: Dict[str, int]) -> SchoolData:
    print(f"\nProcessing school: {school_data['id']} ({school_data.get('SCH_NAME', 'Unknown')})")
    
    available_years = sorted([year for year in school_data['enrollment'].keys() if year is not None], reverse=True)

    if not available_years:
        print(f"Skipping school: {school_data['id']} because no enrollment data is available")
        return school_data

    most_recent_year = available_years[0]

    # Determine entry grade
    grades = school_data['enrollment'][most_recent_year].keys()
    
    # If Pre-K is present, set entry grade to Kindergarten, otherwise find the lowest grade
    if 'Pre-Kindergarten' in grades:
        entry_grade = 'Kindergarten'
    else:
        entry_grade = min(grades, key=lambda x: grade_map.get(x, float('inf')))
    
    school_data['entryGrade'] = entry_grade

    # Calculate averages and collect enrollments
    entry_grade_averages = {'oneYear': 0, 'threeYear': 0, 'fiveYear': 0}
    entry_grade_enrollments = []  # Still collect these for outer bounds
    
    for period, years in [('oneYear', 1), ('threeYear', 3), ('fiveYear', 5)]:
        total = 0
        count = 0
        for year in available_years[:years]:
            if entry_grade in school_data['enrollment'][year]:
                enrollment = school_data['enrollment'][year][entry_grade]
                # Only include positive enrollment values
                if enrollment is not None and enrollment > 0:
                    total += enrollment
                    count += 1
                    entry_grade_enrollments.append(enrollment)
        if count > 0:
            entry_grade_averages[period] = total / count

    # Get the actual average values (filtering out zeros)
    averages = [v for v in entry_grade_averages.values() if v > 0]
    
    if averages:
        entry_grade_estimates = {
            "low": round(min(averages)),
            "high": round(max(averages)),
            "median": round(sorted(averages)[len(averages)//2]),  # Middle value of the averages
            "outer_min": round(min(entry_grade_enrollments)) if entry_grade_enrollments else 0,
            "outer_max": round(max(entry_grade_enrollments)) if entry_grade_enrollments else 0
        }
    else:
        entry_grade_estimates = {
            "low": 0,
            "high": 0,
            "median": 0,
            "outer_min": 0,
            "outer_max": 0
        }
    
    school_data['entryGradeEstimates'] = entry_grade_estimates

    return school_data

def generate_projections(school_data: SchoolData, grade_map: Dict[str, int], forecast_years: List[str]) -> SchoolData:
    enrollment = school_data['enrollment']
    forecast_survival_rates = school_data.get('forecastSurvivalRates', {})
    outer_values = school_data.get('outerValues', {})

    available_years = sorted([year for year in enrollment.keys() if year is not None], reverse=True)
    if not available_years:
        print(f"Skipping projections for school {school_data['id']}: No enrollment data available")
        return school_data
    latest_year = available_years[0]

    # Get all active grades (excluding discontinued ones and Pre-Kindergarten)
    active_grades = [grade for grade, value in enrollment[latest_year].items() 
                    if value != -1 and grade != 'Pre-Kindergarten']
    
    if not active_grades:
        print(f"No active grades found for school {school_data['id']}")
        return school_data
        
    # Sort active grades by grade level
    latest_grades = sorted(active_grades, key=lambda x: grade_map.get(x, float('inf')))
    
    projections = {
        'min': {}, 'median': {}, 'max': {}, 'outer_min': {}, 'outer_max': {}
    }
    
    # Determine entry grade - if Kindergarten exists, use it; otherwise use lowest grade
    if 'Kindergarten' in active_grades:
        entry_grade = 'Kindergarten'
    else:
        entry_grade = min(active_grades, key=lambda x: grade_map.get(x, float('inf')))
    
    school_data['entryGrade'] = entry_grade
    
    entry_grade_estimates = school_data.get('entryGradeEstimates', {})
    
    for year in forecast_years:
        for projection_type in ['min', 'median', 'max']:
            projections[projection_type][year] = {}
            
            # Process each active grade
            for grade in latest_grades:
                if grade == entry_grade:
                    # Use entry grade estimates, ensuring we don't use negative values
                    if projection_type == 'min':
                        value = max(0, entry_grade_estimates.get('low', 0))
                    elif projection_type == 'max':
                        value = max(0, entry_grade_estimates.get('high', 0))
                    else:  # median
                        value = max(0, entry_grade_estimates.get('median', 0))
                else:
                    if grade not in forecast_survival_rates:
                        continue
                            
                    rate = forecast_survival_rates.get(grade, {}).get(projection_type, 1)
                    
                    # Find previous active grade
                    grade_index = latest_grades.index(grade)
                    previous_grade = None
                    for prev_grade in reversed(latest_grades[:grade_index]):
                        if prev_grade in active_grades:
                            previous_grade = prev_grade
                            break
                    
                    if not previous_grade:
                        # Use historical patterns if no previous grade
                        historical_patterns = school_data.get('historicalPatterns', {}).get(grade, {})
                        if historical_patterns:
                            if projection_type == 'min':
                                value = max(0, historical_patterns['min'])
                            elif projection_type == 'max':
                                value = max(0, historical_patterns['max'])
                            else:
                                value = max(0, historical_patterns['median'])
                        else:
                            last_actual = enrollment[latest_year].get(grade, 0)
                            value = max(0, last_actual) if last_actual is not None else 0
                    else:
                        if year == forecast_years[0]:
                            prev_enrollment = enrollment[latest_year].get(previous_grade, 0)
                            value = max(0, prev_enrollment * rate) if prev_enrollment > 0 else 0
                        else:
                            previous_year = forecast_years[forecast_years.index(year)-1]
                            prev_value = projections[projection_type][previous_year].get(previous_grade, 0)
                            value = max(0, prev_value * rate)
                
                projections[projection_type][year][grade] = round(value)
        
        # Set outer bounds
        projections['outer_min'][year] = {
            grade: max(0, outer_values.get(grade, {}).get('outer_min', 0)) 
            for grade in latest_grades
        }
        projections['outer_max'][year] = {
            grade: max(0, outer_values.get(grade, {}).get('outer_max', 0))
            for grade in latest_grades
        }

    school_data['projections'] = projections
    return school_data