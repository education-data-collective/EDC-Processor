from typing import List, Dict
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Constants
POPULATION_THRESHOLD = 2.0
PROJECTION_THRESHOLD = 5.0
MARKET_SHARE_THRESHOLD = 1.4
ENROLLMENT_THRESHOLD = 5.0

def validate_ncessch(ncessch: str, max_length: int = 16) -> str:
    """
    Validate and format NCESSCH identifier to ensure it fits database constraints.
    Now supports extended identifiers with suffixes (e.g., -es, -ms, -hs)
    
    Args:
        ncessch (str): The NCESSCH identifier to validate
        max_length (int): Maximum allowed length (default 16 chars)
        
    Returns:
        str: Validated NCESSCH identifier
        
    Raises:
        ValueError: If NCESSCH is invalid or cannot be properly formatted
    """
    if not ncessch:
        raise ValueError("NCESSCH identifier cannot be empty")
        
    # Remove any whitespace
    cleaned = str(ncessch).strip()
    
    # Check if it's already valid
    if len(cleaned) <= max_length:
        return cleaned
        
    # If too long, this indicates a potential data quality issue
    logger.error(f"NCESSCH {ncessch} exceeds maximum length of {max_length}")
    raise ValueError(f"NCESSCH identifier '{ncessch}' exceeds maximum length of {max_length}")

def calculate_grade_filtered_population(esri_data: Dict, selected_grades: List[str]) -> Dict[str, float]:
    """Calculate population totals for selected grades using ESRI data"""
    def get_age_for_grade(grade: str) -> int:
        if grade == 'Kindergarten':
            return 5
        return int(grade) + 5
    
    if not esri_data or 'ages' not in esri_data or '4_17' not in esri_data['ages']:
        return {'past': 0, 'current': 0, 'future': 0}
    
    age_data = esri_data['ages']['4_17']
    totals = {'past': 0, 'current': 0, 'future': 0}
    
    for grade in selected_grades:
        if grade.lower().startswith('prek') or grade.lower() == 'pre-kindergarten':
            continue
            
        age = get_age_for_grade(grade)
        if 4 <= age <= 17:
            age_index = age - 4
            totals['past'] += age_data['2020'][age_index]
            totals['current'] += age_data['current'][age_index]
            totals['future'] += age_data['future'][age_index]
    
    return totals

def calculate_enrollment(enrollment_data: Dict, selected_grades: List[str]) -> float:
    """Calculate total enrollment for selected grades"""
    if not enrollment_data or not selected_grades:
        return 0
    
    total = 0
    for grade in selected_grades:
        grade_key = f"Grade {grade}" if grade != 'Kindergarten' else 'Kindergarten'
        total += enrollment_data.get(grade_key, 0)
    
    return total

def get_school_grades(enrollment_data: Dict) -> List[str]:
    """Get list of grades with enrollment, excluding Pre-K"""
    if not enrollment_data or 'enrollment_by_grade' not in enrollment_data:
        return []
    
    grades = []
    logger.info("Processing grades from enrollment data")
    
    for grade, count in enrollment_data['enrollment_by_grade'].get('current', {}).items():
        logger.debug(f"Processing grade: {grade} with count: {count}")
        
        # Skip Pre-K grades
        if grade.lower().startswith('prek') or grade.lower() == 'pre-kindergarten':
            logger.debug(f"Skipping Pre-K grade: {grade}")
            continue
            
        if count > 0:
            if grade == 'Kindergarten':
                grades.append('Kindergarten')
            else:
                # Extract numeric grade
                grade_num = grade.replace('Grade ', '')
                grades.append(grade_num)
    
    # Sort grades with Kindergarten first, then numeric grades
    sorted_grades = sorted(grades, key=lambda x: 0 if x == 'Kindergarten' else int(x))
    logger.debug(f"Final sorted grades: {sorted_grades}")
    return sorted_grades

def calculate_market_share(school_enrollment: float, population: float) -> float:
    """Calculate market share percentage"""
    if population <= 0:
        return 0
    return (school_enrollment / population) * 100

def calculate_percent_change(current: float, past: float) -> float:
    """Calculate percentage change between two values"""
    if past <= 0:
        return 0
    return ((current - past) / past) * 100

def get_status(change: float, threshold: float, metric_type: str = 'population') -> str:
    """Determine status based on change and threshold"""
    if metric_type == 'market_share':
        return 'gaining' if change >= threshold else 'losing' if change <= -threshold else 'stable'
    else:
        return 'growing' if change >= threshold else 'declining' if change <= -threshold else 'stable'

def check_newer_school(enrollment_data: Dict) -> bool:
    """Check if school is considered newer based on enrollment data"""
    if not enrollment_data:
        return False
    comparison_data = enrollment_data.get('comparison', {})
    return sum(comparison_data.values()) == 0 if comparison_data else True

