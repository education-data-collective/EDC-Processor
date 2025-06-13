import json
from typing import Dict, Any, List, Union
from .data_structures import SchoolData
from datetime import datetime
from google.cloud import firestore

def get_timestamp(data: Dict[str, Any]) -> Union[datetime, None]:
    timestamp = data.get('timestamp')
    if isinstance(timestamp, datetime):
        return timestamp
    elif isinstance(timestamp, (firestore.SERVER_TIMESTAMP, firestore._helpers.ServerTimestampSentinel)):
        return datetime.now()  # Use current time as a fallback
    elif isinstance(timestamp, str):
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            return None
    else:
        return None

def process_user_data(school_data: SchoolData, user_data_list: List[Union[Dict[str, Any], str]]) -> SchoolData:
    # Convert any string items to dictionaries
    parsed_user_data = []
    for item in user_data_list:
        if isinstance(item, str):
            try:
                parsed_item = json.loads(item)
                parsed_user_data.append(parsed_item)
            except json.JSONDecodeError:
                continue  # Skip invalid JSON strings
        else:
            parsed_user_data.append(item)

    # Sort parsed_user_data by timestamp in descending order (most recent first)
    sorted_user_data = sorted(
        parsed_user_data, 
        key=lambda x: get_timestamp(x) or datetime.min,
        reverse=True
    )

    # Keep track of years we've processed to avoid duplicates
    processed_years = set()

    for user_data in sorted_user_data:
        year = user_data.get('year')
        if not year:
            continue

        # Skip if we've already processed this year
        if year in processed_years:
            continue

        processed_years.add(year)

        if year not in school_data['enrollment']:
            school_data['enrollment'][year] = {}

        grades = user_data.get('grades', {})
        for grade, enrollment in grades.items():
            if enrollment is not None:
                school_data['enrollment'][year][grade] = enrollment

    # Update school name if provided in the latest user data
    if sorted_user_data and 'schoolName' in sorted_user_data[0]:
        school_data['school_name'] = sorted_user_data[0]['schoolName']

    return school_data