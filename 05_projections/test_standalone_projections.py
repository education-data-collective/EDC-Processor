#!/usr/bin/env python3
"""
Unit Test: Validate Standalone Projection Functions

This test validates the standalone projection functions in generate_projections_csv.py
against expected behavior and mathematical correctness.
"""

import sys
import json
import unittest
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Import standalone functions
from generate_projections_csv import (
    calculate_survival_rates,
    calculate_forecast_survival_rates,
    calculate_entry_grade_estimates,
    generate_projections,
    GRADE_MAP,
    generate_forecast_years,
    get_most_recent_year
)

class TestStandaloneProjections(unittest.TestCase):
    
    def setUp(self):
        """Set up test data"""
        self.elementary_school = {
            'id': 12345,
            'ncessch': 'ELEM123456',
            'school_name': 'Test Elementary School',
            'enrollment': {
                '2019-2020': {
                    'Kindergarten': 25,
                    'Grade 1': 23,
                    'Grade 2': 22,
                    'Grade 3': 24,
                    'Grade 4': 21,
                    'Grade 5': 20
                },
                '2020-2021': {
                    'Kindergarten': 27,
                    'Grade 1': 24,  # Good retention from previous K
                    'Grade 2': 22,  # ~95% retention 
                    'Grade 3': 21,  # ~95% retention
                    'Grade 4': 23,  # ~95% retention  
                    'Grade 5': 20   # ~95% retention
                },
                '2021-2022': {
                    'Kindergarten': 26,
                    'Grade 1': 26,  # Good retention from previous K
                    'Grade 2': 23,  # ~95% retention
                    'Grade 3': 21,  # ~95% retention
                    'Grade 4': 20,  # ~95% retention
                    'Grade 5': 22   # ~95% retention
                }
            }
        }
        
        self.high_school = {
            'id': 67890,
            'ncessch': 'HIGH123456', 
            'school_name': 'Test High School',
            'enrollment': {
                '2019-2020': {
                    'Grade 9': 100,
                    'Grade 10': 95,
                    'Grade 11': 90,
                    'Grade 12': 85
                },
                '2020-2021': {
                    'Grade 9': 105,
                    'Grade 10': 98,  # 98% retention
                    'Grade 11': 92,  # ~97% retention
                    'Grade 12': 88   # ~98% retention
                },
                '2021-2022': {
                    'Grade 9': 110,
                    'Grade 10': 102, # ~97% retention
                    'Grade 11': 95,  # ~97% retention
                    'Grade 12': 90   # ~98% retention
                }
            }
        }

    def test_utility_functions(self):
        """Test utility functions"""
        print("\n🧪 Testing utility functions...")
        
        # Test forecast year generation
        forecast_years = generate_forecast_years('2021-2022', 3)
        expected_years = ['2022-2023', '2023-2024', '2024-2025']
        self.assertEqual(forecast_years, expected_years)
        
        # Test most recent year identification
        most_recent = get_most_recent_year(self.elementary_school['enrollment'])
        self.assertEqual(most_recent, '2021-2022')
        
        print(f"✅ Forecast years: {forecast_years}")
        print(f"✅ Most recent year: {most_recent}")

    def test_survival_rates_calculation(self):
        """Test survival rate calculations"""
        print("\n🧪 Testing survival rates calculation...")
        
        school_data = self.elementary_school.copy()
        result = calculate_survival_rates(school_data, GRADE_MAP)
        
        # Check basic structure
        self.assertIn('survivalRates', result)
        self.assertIn('entryGrade', result)
        
        # Entry grade should be Kindergarten for elementary
        self.assertEqual(result['entryGrade'], 'Kindergarten')
        
        # Check one-year survival rates exist
        survival_rates = result['survivalRates']
        if 'oneYear' in survival_rates:
            one_year = survival_rates['oneYear']
            print(f"One-year survival rates: {json.dumps(one_year, indent=2)}")
            
            # Kindergarten shouldn't have survival rate (it's entry grade)
            self.assertNotIn('Kindergarten', one_year)
            
            # Other grades should have reasonable rates (between 0.5 and 1.5)
            for grade, rate in one_year.items():
                self.assertGreaterEqual(rate, 0.5, f"{grade} survival rate too low: {rate}")
                self.assertLessEqual(rate, 1.5, f"{grade} survival rate too high: {rate}")
        
        print("✅ Survival rates calculation passed")

    def test_entry_grade_estimates(self):
        """Test entry grade estimate calculations"""
        print("\n🧪 Testing entry grade estimates...")
        
        school_data = self.elementary_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        result = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        # Check structure
        self.assertIn('entryGradeEstimates', result)
        
        estimates = result['entryGradeEstimates']
        required_fields = ['low', 'high', 'median']
        
        for field in required_fields:
            self.assertIn(field, estimates)
            self.assertIsInstance(estimates[field], (int, float))
            self.assertGreaterEqual(estimates[field], 0)
        
        # High should be >= median >= low (with tolerance for ties)
        self.assertGreaterEqual(estimates['high'], estimates['median'])
        self.assertGreaterEqual(estimates['median'], estimates['low'])
        
        print(f"Entry grade estimates: {json.dumps(estimates, indent=2)}")
        print("✅ Entry grade estimates calculation passed")

    def test_forecast_survival_rates(self):
        """Test forecast survival rate calculations"""
        print("\n🧪 Testing forecast survival rates...")
        
        school_data = self.elementary_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        result = calculate_forecast_survival_rates(school_data)
        
        # Check structure
        self.assertIn('forecastSurvivalRates', result)
        
        forecast_rates = result['forecastSurvivalRates']
        
        # Should have rates for each grade
        for grade in school_data['enrollment']['2021-2022'].keys():
            if grade in forecast_rates:
                grade_rates = forecast_rates[grade]
                required_fields = ['min', 'median', 'max']
                
                for field in required_fields:
                    self.assertIn(field, grade_rates)
                    self.assertIsInstance(grade_rates[field], (int, float))
                    self.assertGreater(grade_rates[field], 0)
        
        print(f"Sample forecast rates for Grade 2: {forecast_rates.get('Grade 2', 'N/A')}")
        print("✅ Forecast survival rates calculation passed")

    def test_full_projections_pipeline(self):
        """Test the complete projections pipeline"""
        print("\n🧪 Testing full projections pipeline...")
        
        # Test elementary school
        school_data = self.elementary_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        school_data = calculate_forecast_survival_rates(school_data)
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        forecast_years = generate_forecast_years('2021-2022', 3)
        result = generate_projections(school_data, GRADE_MAP, forecast_years)
        
        # Check final structure
        self.assertIn('projections', result)
        
        projections = result['projections']
        projection_types = ['min', 'median', 'max', 'outer_min', 'outer_max']
        
        for proj_type in projection_types:
            self.assertIn(proj_type, projections)
            
            # Check each forecast year exists
            for year in forecast_years:
                self.assertIn(year, projections[proj_type])
                
                # Check grades exist
                year_projections = projections[proj_type][year]
                self.assertGreater(len(year_projections), 0)
                
                # All enrollments should be non-negative integers
                for grade, enrollment in year_projections.items():
                    self.assertIsInstance(enrollment, int)
                    self.assertGreaterEqual(enrollment, 0)
        
        # Print sample projections
        print(f"Sample projections for {forecast_years[0]}:")
        for proj_type in ['min', 'median', 'max']:
            print(f"  {proj_type}: {projections[proj_type][forecast_years[0]]}")
        
        print("✅ Full projections pipeline passed")

    def test_high_school_projections(self):
        """Test projections for high school data"""
        print("\n🧪 Testing high school projections...")
        
        school_data = self.high_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        school_data = calculate_forecast_survival_rates(school_data)
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        forecast_years = generate_forecast_years('2021-2022', 2)
        result = generate_projections(school_data, GRADE_MAP, forecast_years)
        
        # Entry grade should be Grade 9 for high school
        self.assertEqual(result['entryGrade'], 'Grade 9')
        
        # Check projections exist
        self.assertIn('projections', result)
        projections = result['projections']
        
        # Sample the median projections
        median_first_year = projections['median'][forecast_years[0]]
        print(f"High school projections for {forecast_years[0]}: {median_first_year}")
        
        # Grade 9 should use entry grade estimates
        # Other grades should use survival rates from previous grades
        self.assertIn('Grade 9', median_first_year)
        self.assertIn('Grade 10', median_first_year)
        
        print("✅ High school projections passed")

    def test_mathematical_consistency(self):
        """Test mathematical consistency of projections"""
        print("\n🧪 Testing mathematical consistency...")
        
        school_data = self.elementary_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        school_data = calculate_forecast_survival_rates(school_data)
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        forecast_years = generate_forecast_years('2021-2022', 3)
        result = generate_projections(school_data, GRADE_MAP, forecast_years)
        
        projections = result['projections']
        
        # For each year and grade, min <= median <= max
        for year in forecast_years:
            for grade in projections['median'][year].keys():
                min_val = projections['min'][year][grade]
                median_val = projections['median'][year][grade]
                max_val = projections['max'][year][grade]
                
                self.assertLessEqual(min_val, median_val, 
                    f"Min > Median for {grade} in {year}: {min_val} > {median_val}")
                self.assertLessEqual(median_val, max_val,
                    f"Median > Max for {grade} in {year}: {median_val} > {max_val}")
        
        print("✅ Mathematical consistency verified")

    def test_edge_cases(self):
        """Test edge cases"""
        print("\n🧪 Testing edge cases...")
        
        # Test with minimal data
        minimal_school = {
            'id': 999,
            'ncessch': 'MIN123',
            'school_name': 'Minimal School',
            'enrollment': {
                '2021-2022': {
                    'Kindergarten': 10
                }
            }
        }
        
        school_data = minimal_school.copy()
        school_data = calculate_survival_rates(school_data, GRADE_MAP)
        school_data = calculate_forecast_survival_rates(school_data)
        school_data = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        forecast_years = generate_forecast_years('2021-2022', 2)
        result = generate_projections(school_data, GRADE_MAP, forecast_years)
        
        # Should still produce projections
        self.assertIn('projections', result)
        self.assertEqual(result['entryGrade'], 'Kindergarten')
        
        print("✅ Edge cases handled correctly")

def main():
    print("="*60)
    print("STANDALONE PROJECTION FUNCTIONS TEST")
    print("="*60)
    
    unittest.main(verbosity=2)

if __name__ == "__main__":
    main() 