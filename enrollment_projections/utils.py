from typing import Dict, List
from datetime import datetime, timedelta

GRADE_MAP: Dict[str, int] = {
    'Kindergarten': 0,
    'Grade 1': 1,
    'Grade 2': 2,
    'Grade 3': 3,
    'Grade 4': 4,
    'Grade 5': 5,
    'Grade 6': 6,
    'Grade 7': 7,
    'Grade 8': 8,
    'Grade 9': 9,
    'Grade 10': 10,
    'Grade 11': 11,
    'Grade 12': 12
}

PREVIOUS_GRADE_MAP = {
    'Grade 1': 'Kindergarten',
    'Grade 2': 'Grade 1',
    'Grade 3': 'Grade 2',
    'Grade 4': 'Grade 3',
    'Grade 5': 'Grade 4',
    'Grade 6': 'Grade 5',
    'Grade 7': 'Grade 6',
    'Grade 8': 'Grade 7',
    'Grade 9': 'Grade 8',
    'Grade 10': 'Grade 9',
    'Grade 11': 'Grade 10',
    'Grade 12': 'Grade 11'
}

def generate_forecast_years(most_recent_year: str, num_years: int = 5) -> List[str]:
    start_year = int(most_recent_year.split('-')[0]) + 1
    return [f"{year}-{year+1}" for year in range(start_year, start_year + num_years)]

def get_most_recent_year(enrollment_data: Dict[str, Dict]) -> str:
    return max(enrollment_data.keys())