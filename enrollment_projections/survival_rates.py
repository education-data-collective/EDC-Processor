from typing import Dict, List
from .data_structures import SchoolData
import statistics as stats
from .utils import generate_forecast_years, get_most_recent_year, PREVIOUS_GRADE_MAP
import json

def calculate_survival_rates(school_data: SchoolData, grade_map: Dict[str, int]) -> SchoolData:

   available_years = sorted([year for year in school_data['enrollment'].keys() if year is not None], reverse=True)
   if not available_years:
       print(f"Skipping school: {school_data['id']} because it does not have any enrollment data")
       return school_data

   # Get discontinued grades from latest year
   latest_year = available_years[0]
   discontinued_grades = {
       grade for grade, enrollment in school_data['enrollment'][latest_year].items() 
       if enrollment == -1
   }

   grades_to_analyze = sorted([
       grade for grade in school_data['enrollment'][available_years[0]].keys() 
       if isinstance(school_data['enrollment'][available_years[0]][grade], (int, float)) 
       and school_data['enrollment'][available_years[0]][grade] >= 0
   ], key=lambda x: grade_map.get(x, float('inf')))

   if not grades_to_analyze:
       print(f"Skipping school: {school_data['id']} because it does not have any enrollment data for the most recent year: {available_years[0]}")
       return school_data

   entry_grade = min(grades_to_analyze, key=lambda x: grade_map.get(x, float('inf')))
   school_data['entryGrade'] = entry_grade

   survival_rates = {}
   historical_patterns = {}

   # Store historical enrollment patterns
   for grade in grades_to_analyze:
       historical_enrollments = [
           school_data['enrollment'][year][grade] 
           for year in available_years 
           if grade in school_data['enrollment'][year] 
           and school_data['enrollment'][year][grade] > 0
       ]
       if historical_enrollments:
           historical_patterns[grade] = {
               'min': min(historical_enrollments),
               'max': max(historical_enrollments),
               'median': stats.median(historical_enrollments)
           }

   # 1-year survival rates
   if len(available_years) >= 2:
       survival_rates['oneYear'] = {}
       current_year = available_years[0]
       previous_year = available_years[1]
       for grade in grades_to_analyze:
           if grade == entry_grade or grade in discontinued_grades:
               continue
           previous_grade = PREVIOUS_GRADE_MAP.get(grade)
           if previous_grade and previous_grade not in discontinued_grades:
               current_enrollment = school_data['enrollment'][current_year].get(grade, 0)
               previous_enrollment = school_data['enrollment'][previous_year].get(previous_grade, 0)
               if previous_enrollment > 0 and current_enrollment >= 0:
                   survival_rates['oneYear'][grade] = current_enrollment / previous_enrollment

   # 3-year survival rates
   if len(available_years) >= 4:
       survival_rates['threeYear'] = {}
       for grade in grades_to_analyze:
           if grade == entry_grade or grade in discontinued_grades:
               continue
           previous_grade = PREVIOUS_GRADE_MAP.get(grade)
           if previous_grade and previous_grade not in discontinued_grades:
               current_sum = sum(enrollment for year in available_years[:3]
                               for enrollment in [school_data['enrollment'].get(year, {}).get(grade, 0)]
                               if enrollment >= 0)
               previous_sum = sum(enrollment for year in available_years[1:4]
                                for enrollment in [school_data['enrollment'].get(year, {}).get(previous_grade, 0)]
                                if enrollment >= 0)
               if previous_sum > 0:
                   survival_rates['threeYear'][grade] = current_sum / previous_sum

   # 5-year survival rates
   if len(available_years) >= 6:
       survival_rates['fiveYear'] = {}
       for grade in grades_to_analyze:
           if grade == entry_grade or grade in discontinued_grades:
               continue
           previous_grade = PREVIOUS_GRADE_MAP.get(grade)
           if previous_grade and previous_grade not in discontinued_grades:
               current_sum = sum(enrollment for year in available_years[:5]
                               for enrollment in [school_data['enrollment'].get(year, {}).get(grade, 0)]
                               if enrollment >= 0)
               previous_sum = sum(enrollment for year in available_years[1:6]
                                for enrollment in [school_data['enrollment'].get(year, {}).get(previous_grade, 0)]
                                if enrollment >= 0)
               if previous_sum > 0:
                   survival_rates['fiveYear'][grade] = current_sum / previous_sum

   school_data['survivalRates'] = survival_rates
   school_data['historicalPatterns'] = historical_patterns
   print(f"Survival rates calculated for school: {school_data['id']}")

   return school_data

def calculate_outer_max_min(school_data: SchoolData) -> SchoolData:
    outer_values = {}
    
    available_years = sorted(school_data['enrollment'].keys(), reverse=True)
    if not available_years:
        print(f"Warning: No enrollment data found for school {school_data['id']}")
        return school_data
    
    for grade in school_data['enrollment'][available_years[0]].keys():
        enrollments = []
        for year in available_years:
            if grade in school_data['enrollment'][year]:
                enrollment = school_data['enrollment'][year][grade]
                if isinstance(enrollment, (int, float)) and enrollment >= 0:
                    enrollments.append(enrollment)
            
        if enrollments:
            outer_values[grade] = {
                "outer_max": max(enrollments),
                "outer_min": min(enrollments)
            }
        else:
            outer_values[grade] = {
                "outer_max": 0,
                "outer_min": 0
            }
    
    school_data['outerValues'] = outer_values
    return school_data

def calculate_forecast_survival_rates(school_data: SchoolData) -> SchoolData:
   school_data = calculate_outer_max_min(school_data)
   
   survival_rates = school_data.get('survivalRates', {})
   outer_values = school_data.get('outerValues', {})
   forecast_survival_rates = {}
   
   # Get discontinued grades
   latest_year = max(school_data['enrollment'].keys())
   discontinued_grades = {
       grade for grade, enrollment in school_data['enrollment'][latest_year].items() 
       if enrollment == -1
   }
   
   # Only process active grades
   for grade in outer_values.keys():
       if grade in discontinued_grades:
           continue
           
       one_year_rate = survival_rates.get('oneYear', {}).get(grade, 0)
       three_year_rate = survival_rates.get('threeYear', {}).get(grade, 0)
       five_year_rate = survival_rates.get('fiveYear', {}).get(grade, 0)
       
       # If no rates available, use historical retention
       if not any([one_year_rate, three_year_rate, five_year_rate]):
           historical_enrollments = [
               val for year in school_data['enrollment']
               for val in [school_data['enrollment'][year].get(grade)]
               if val and val > 0
           ]
           if historical_enrollments:
               min_enrollment = min(historical_enrollments)
               max_enrollment = max(historical_enrollments)
               median_enrollment = stats.median(historical_enrollments)
               forecast_survival_rates[grade] = {
                   "median": 1.0,  # Maintain current enrollment
                   "min": min_enrollment / median_enrollment,
                   "max": max_enrollment / median_enrollment,
                   "outer_max": outer_values[grade]["outer_max"],
                   "outer_min": outer_values[grade]["outer_min"]
               }
               continue
       
       # If 3-year rate is missing, use 1-year rate
       if three_year_rate == 0 and one_year_rate != 0:
           three_year_rate = one_year_rate
       
       # If 5-year rate is missing, use average of available rates
       if five_year_rate == 0:
           available_rates = [r for r in [one_year_rate, three_year_rate] if r != 0]
           five_year_rate = sum(available_rates) / len(available_rates) if available_rates else 0
       
       rates = [one_year_rate, three_year_rate, five_year_rate]
       non_zero_rates = [r for r in rates if r != 0]
       
       if non_zero_rates:
           forecast_survival_rates[grade] = {
               "median": stats.median(non_zero_rates),
               "min": min(non_zero_rates),
               "max": max(non_zero_rates),
               "outer_max": outer_values[grade]["outer_max"],
               "outer_min": outer_values[grade]["outer_min"]
           }
       else:
           forecast_survival_rates[grade] = {
               "median": 1.0,  # Default to maintaining current enrollment
               "min": 0.9,     # Small decrease
               "max": 1.1,     # Small increase
               "outer_max": outer_values[grade]["outer_max"],
               "outer_min": outer_values[grade]["outer_min"]
           }
   
   school_data['forecastSurvivalRates'] = forecast_survival_rates
   return school_data