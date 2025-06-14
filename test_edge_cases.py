#!/usr/bin/env python3
"""
Comprehensive Edge Case Tests for Enhanced Projection Functions
Tests all the edge cases and features from the original enrollment_projections package
"""

import unittest
import json
from generate_projections_csv import (
    calculate_survival_rates, calculate_entry_grade_estimates, 
    calculate_forecast_survival_rates, generate_projections,
    generate_forecast_years, get_most_recent_year, GRADE_MAP
)

class TestEdgeCases(unittest.TestCase):
    
    def test_discontinued_grades(self):
        """Test handling of discontinued grades (enrollment = -1)"""
        print("🧪 Testing discontinued grades handling...")
        
        school_data = {
            'id': 'DISC001',
            'ncessch': '123456789012',
            'school_name': 'School with Discontinued Grades',
            'enrollment': {
                '2021-2022': {
                    'Kindergarten': 25,
                    'Grade 1': 23,
                    'Grade 2': 22,
                    'Grade 3': -1,  # Discontinued grade
                    'Grade 4': 20,
                    'Grade 5': 19
                },
                '2020-2021': {
                    'Kindergarten': 27,
                    'Grade 1': 24,
                    'Grade 2': 23,
                    'Grade 3': 21,  # Was active before
                    'Grade 4': 21,
                    'Grade 5': 20
                }
            }
        }
        
        # Test survival rates calculation
        result = calculate_survival_rates(school_data, GRADE_MAP)
        
        # Grade 3 should be excluded from survival rate calculations
        self.assertNotIn('Grade 3', result['survivalRates'].get('oneYear', {}))
        
        # Grade 4 should not have survival rate calculated because its previous grade (Grade 3) is discontinued
        self.assertNotIn('Grade 4', result['survivalRates'].get('oneYear', {}))
        
        # Discontinued grades should be tracked
        self.assertIn('Grade 3', result['discontinuedGrades'])
        
        print("✅ Discontinued grades handled correctly")
    
    def test_pre_kindergarten_handling(self):
        """Test proper handling of Pre-Kindergarten"""
        print("🧪 Testing Pre-Kindergarten handling...")
        
        school_data = {
            'id': 'PK001',
            'ncessch': '123456789013',
            'school_name': 'School with Pre-K',
            'enrollment': {
                '2021-2022': {
                    'Pre-Kindergarten': 15,
                    'Kindergarten': 25,
                    'Grade 1': 23,
                    'Grade 2': 22
                },
                '2020-2021': {
                    'Pre-Kindergarten': 18,
                    'Kindergarten': 27,
                    'Grade 1': 24,
                    'Grade 2': 23
                }
            }
        }
        
        # Test entry grade identification
        result = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        # Entry grade should be Kindergarten, not Pre-K
        self.assertEqual(result['entryGrade'], 'Kindergarten')
        
        # Test projections exclude Pre-K
        forecast_years = generate_forecast_years('2021-2022', 3)
        result = calculate_forecast_survival_rates(result)
        result = generate_projections(result, GRADE_MAP, forecast_years)
        
        # Pre-K should not appear in projections
        for year in forecast_years:
            for projection_type in ['min', 'median', 'max']:
                self.assertNotIn('Pre-Kindergarten', result['projections'][projection_type][year])
        
        print("✅ Pre-Kindergarten handled correctly")
    
    def test_multi_year_survival_rates(self):
        """Test 3-year and 5-year survival rate calculations"""
        print("🧪 Testing multi-year survival rates...")
        
        # Create data with 6 years for 5-year survival rates
        school_data = {
            'id': 'MULTI001',
            'ncessch': '123456789014',
            'school_name': 'Multi-Year Test School',
            'enrollment': {
                '2021-2022': {'Kindergarten': 25, 'Grade 1': 24, 'Grade 2': 23, 'Grade 3': 22},
                '2020-2021': {'Kindergarten': 26, 'Grade 1': 25, 'Grade 2': 24, 'Grade 3': 23},
                '2019-2020': {'Kindergarten': 27, 'Grade 1': 26, 'Grade 2': 25, 'Grade 3': 24},
                '2018-2019': {'Kindergarten': 28, 'Grade 1': 27, 'Grade 2': 26, 'Grade 3': 25},
                '2017-2018': {'Kindergarten': 29, 'Grade 1': 28, 'Grade 2': 27, 'Grade 3': 26},
                '2016-2017': {'Kindergarten': 30, 'Grade 1': 29, 'Grade 2': 28, 'Grade 3': 27}
            }
        }
        
        result = calculate_survival_rates(school_data, GRADE_MAP)
        
        # Should have 1-year, 3-year, and 5-year survival rates
        self.assertIn('oneYear', result['survivalRates'])
        self.assertIn('threeYear', result['survivalRates'])
        self.assertIn('fiveYear', result['survivalRates'])
        
        # Check that rates are calculated for non-entry grades
        for period in ['oneYear', 'threeYear', 'fiveYear']:
            self.assertIn('Grade 1', result['survivalRates'][period])
            self.assertIn('Grade 2', result['survivalRates'][period])
            self.assertIn('Grade 3', result['survivalRates'][period])
        
        print("✅ Multi-year survival rates calculated correctly")
    
    def test_historical_patterns(self):
        """Test historical enrollment pattern analysis"""
        print("🧪 Testing historical patterns...")
        
        school_data = {
            'id': 'HIST001',
            'ncessch': '123456789015',
            'school_name': 'Historical Patterns School',
            'enrollment': {
                '2021-2022': {'Kindergarten': 30, 'Grade 1': 25, 'Grade 2': 20},
                '2020-2021': {'Kindergarten': 25, 'Grade 1': 30, 'Grade 2': 25},
                '2019-2020': {'Kindergarten': 35, 'Grade 1': 20, 'Grade 2': 30}
            }
        }
        
        result = calculate_survival_rates(school_data, GRADE_MAP)
        
        # Should have historical patterns
        self.assertIn('historicalPatterns', result)
        
        # Check Kindergarten patterns
        kg_patterns = result['historicalPatterns']['Kindergarten']
        self.assertEqual(kg_patterns['min'], 25)
        self.assertEqual(kg_patterns['max'], 35)
        self.assertEqual(kg_patterns['median'], 30)
        
        print("✅ Historical patterns calculated correctly")
    
    def test_outer_bounds_calculation(self):
        """Test proper outer bounds (historical min/max) calculation"""
        print("🧪 Testing outer bounds calculation...")
        
        school_data = {
            'id': 'OUTER001',
            'ncessch': '123456789016',
            'school_name': 'Outer Bounds School',
            'enrollment': {
                '2021-2022': {'Kindergarten': 25, 'Grade 1': 30},
                '2020-2021': {'Kindergarten': 35, 'Grade 1': 20},
                '2019-2020': {'Kindergarten': 20, 'Grade 1': 35}
            }
        }
        
        result = calculate_survival_rates(school_data, GRADE_MAP)
        result = calculate_forecast_survival_rates(result)
        
        # Should have outer values
        self.assertIn('outerValues', result)
        
        # Check outer bounds
        kg_outer = result['outerValues']['Kindergarten']
        self.assertEqual(kg_outer['outer_min'], 20)
        self.assertEqual(kg_outer['outer_max'], 35)
        
        # Check that forecast survival rates include outer bounds
        kg_forecast = result['forecastSurvivalRates']['Kindergarten']
        self.assertEqual(kg_forecast['outer_min'], 20)
        self.assertEqual(kg_forecast['outer_max'], 35)
        
        print("✅ Outer bounds calculated correctly")
    
    def test_fallback_mechanisms(self):
        """Test fallback mechanisms when survival rates are unavailable"""
        print("🧪 Testing fallback mechanisms...")
        
        # School with only one year of data (no survival rates possible)
        school_data = {
            'id': 'FALL001',
            'ncessch': '123456789017',
            'school_name': 'Fallback School',
            'enrollment': {
                '2021-2022': {'Kindergarten': 25, 'Grade 1': 24, 'Grade 2': 23}
            }
        }
        
        result = calculate_survival_rates(school_data, GRADE_MAP)
        result = calculate_forecast_survival_rates(result)
        
        # Should still have forecast survival rates using fallback logic
        self.assertIn('forecastSurvivalRates', result)
        
        # Should use default values when no historical data
        for grade in ['Kindergarten', 'Grade 1', 'Grade 2']:
            forecast = result['forecastSurvivalRates'][grade]
            self.assertIn('median', forecast)
            self.assertIn('min', forecast)
            self.assertIn('max', forecast)
        
        print("✅ Fallback mechanisms working correctly")
    
    def test_entry_grade_estimates_edge_cases(self):
        """Test entry grade estimates with various edge cases"""
        print("🧪 Testing entry grade estimates edge cases...")
        
        # Test with varying entry grade enrollments
        school_data = {
            'id': 'ENTRY001',
            'ncessch': '123456789018',
            'school_name': 'Entry Grade Test School',
            'enrollment': {
                '2021-2022': {'Kindergarten': 30},
                '2020-2021': {'Kindergarten': 25},
                '2019-2020': {'Kindergarten': 35},
                '2018-2019': {'Kindergarten': 20},
                '2017-2018': {'Kindergarten': 40}
            }
        }
        
        result = calculate_entry_grade_estimates(school_data, GRADE_MAP)
        
        estimates = result['entryGradeEstimates']
        
        # Should calculate proper estimates
        self.assertGreaterEqual(estimates['high'], estimates['low'])  # Can be equal if all averages are the same
        self.assertEqual(estimates['outer_min'], 20)
        self.assertEqual(estimates['outer_max'], 40)
        
        print("✅ Entry grade estimates edge cases handled correctly")
    
    def test_complex_projection_scenario(self):
        """Test complex projection scenario with multiple edge cases"""
        print("🧪 Testing complex projection scenario...")
        
        school_data = {
            'id': 'COMPLEX001',
            'ncessch': '123456789019',
            'school_name': 'Complex Scenario School',
            'enrollment': {
                '2021-2022': {
                    'Pre-Kindergarten': 15,  # Should be excluded
                    'Kindergarten': 25,
                    'Grade 1': 24,
                    'Grade 2': 23,
                    'Grade 3': -1,  # Discontinued
                    'Grade 4': 21,
                    'Grade 5': 20
                },
                '2020-2021': {
                    'Pre-Kindergarten': 18,
                    'Kindergarten': 27,
                    'Grade 1': 25,
                    'Grade 2': 24,
                    'Grade 3': 23,  # Was active
                    'Grade 4': 22,
                    'Grade 5': 21
                },
                '2019-2020': {
                    'Kindergarten': 30,
                    'Grade 1': 26,
                    'Grade 2': 25,
                    'Grade 3': 24,
                    'Grade 4': 23,
                    'Grade 5': 22
                }
            }
        }
        
        # Run full pipeline
        result = calculate_survival_rates(school_data, GRADE_MAP)
        result = calculate_entry_grade_estimates(result, GRADE_MAP)
        result = calculate_forecast_survival_rates(result)
        
        forecast_years = generate_forecast_years('2021-2022', 3)
        result = generate_projections(result, GRADE_MAP, forecast_years)
        
        # Verify complex scenario handling
        self.assertEqual(result['entryGrade'], 'Kindergarten')
        self.assertIn('Grade 3', result['discontinuedGrades'])
        self.assertIn('projections', result)
        
        # Check that projections exist for active grades only
        first_year_projections = result['projections']['median'][forecast_years[0]]
        self.assertIn('Kindergarten', first_year_projections)
        self.assertIn('Grade 1', first_year_projections)
        self.assertIn('Grade 2', first_year_projections)
        self.assertNotIn('Grade 3', first_year_projections)  # Discontinued
        self.assertNotIn('Pre-Kindergarten', first_year_projections)  # Excluded
        
        print("✅ Complex projection scenario handled correctly")

def main():
    print("=" * 60)
    print("COMPREHENSIVE EDGE CASE TESTS")
    print("=" * 60)
    
    unittest.main(verbosity=2)

if __name__ == '__main__':
    main() 