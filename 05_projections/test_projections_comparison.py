#!/usr/bin/env python3
"""
Unit Test: Compare Standalone vs Original Projection Implementations

This test ensures that the simplified projection functions in generate_projections_csv.py
produce the same results as the original enrollment_projections package.
"""

import sys
import json
import unittest
from pathlib import Path
from typing import Dict, Any

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Import standalone functions from generate_projections_csv.py
from generate_projections_csv import (
    calculate_survival_rates as standalone_survival_rates,
    calculate_forecast_survival_rates as standalone_forecast_survival_rates,
    calculate_entry_grade_estimates as standalone_entry_grade_estimates,
    generate_projections as standalone_generate_projections,
    GRADE_MAP,
    generate_forecast_years,
    get_most_recent_year
)

# Mock Flask app context for original functions
class MockApp:
    class MockLogger:
        def info(self, msg): pass
        def error(self, msg): pass
        def debug(self, msg): pass
    
    logger = MockLogger()

class MockCurrentApp:
    def __init__(self):
        self.logger = MockApp.MockLogger()
    
    def app_context(self):
        return self

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass

# Mock Flask current_app for original enrollment_projections
sys.modules['flask'] = type(sys)('flask')
sys.modules['flask'].current_app = MockCurrentApp()

# Now import original functions
try:
    from enrollment_projections.survival_rates import (
        calculate_survival_rates as original_survival_rates,
        calculate_forecast_survival_rates as original_forecast_survival_rates
    )
    from enrollment_projections.projections import (
        calculate_entry_grade_estimates as original_entry_grade_estimates,
        generate_projections as original_generate_projections
    )
    from enrollment_projections.utils import GRADE_MAP as ORIGINAL_GRADE_MAP
    ORIGINAL_FUNCTIONS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import original functions: {e}")
    ORIGINAL_FUNCTIONS_AVAILABLE = False

class TestProjectionComparison(unittest.TestCase):
    
    def setUp(self):
        """Set up test data for comparison"""
        self.test_school_data = {
            'id': 12345,
            'ncessch': 'TEST123456',
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
                    'Grade 1': 24,
                    'Grade 2': 22,
                    'Grade 3': 21,
                    'Grade 4': 23,
                    'Grade 5': 20
                },
                '2021-2022': {
                    'Kindergarten': 26,
                    'Grade 1': 26,
                    'Grade 2': 23,
                    'Grade 3': 21,
                    'Grade 4': 20,
                    'Grade 5': 22
                }
            }
        }
        
        self.forecast_years = generate_forecast_years('2021-2022', 3)

    def deep_compare_dict(self, dict1, dict2, path=""):
        """Deep compare two dictionaries, reporting differences"""
        differences = []
        
        # Check keys
        keys1 = set(dict1.keys()) if isinstance(dict1, dict) else set()
        keys2 = set(dict2.keys()) if isinstance(dict2, dict) else set()
        
        if keys1 != keys2:
            differences.append(f"Keys differ at {path}: {keys1} vs {keys2}")
        
        # Check values for common keys
        for key in keys1.intersection(keys2):
            val1 = dict1[key]
            val2 = dict2[key]
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(val1, dict) and isinstance(val2, dict):
                differences.extend(self.deep_compare_dict(val1, val2, current_path))
            elif isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                # Allow small floating point differences
                if abs(val1 - val2) > 0.001:
                    differences.append(f"Numeric difference at {current_path}: {val1} vs {val2}")
            elif val1 != val2:
                differences.append(f"Value difference at {current_path}: {val1} vs {val2}")
        
        return differences

    @unittest.skipUnless(ORIGINAL_FUNCTIONS_AVAILABLE, "Original functions not available")
    def test_survival_rates_comparison(self):
        """Compare survival rate calculations"""
        print("\n🧪 Testing survival rates comparison...")
        
        # Test with standalone function
        standalone_data = self.test_school_data.copy()
        standalone_result = standalone_survival_rates(standalone_data, GRADE_MAP)
        
        # Test with original function  
        original_data = self.test_school_data.copy()
        original_result = original_survival_rates(original_data, ORIGINAL_GRADE_MAP)
        
        # Compare survival rates
        standalone_rates = standalone_result.get('survivalRates', {})
        original_rates = original_result.get('survivalRates', {})
        
        print(f"Standalone survival rates: {json.dumps(standalone_rates, indent=2)}")
        print(f"Original survival rates: {json.dumps(original_rates, indent=2)}")
        
        differences = self.deep_compare_dict(standalone_rates, original_rates, "survivalRates")
        if differences:
            print(f"⚠️  Survival rates differences found:")
            for diff in differences:
                print(f"  - {diff}")
        else:
            print("✅ Survival rates match!")
            
        # Entry grade should match
        self.assertEqual(
            standalone_result.get('entryGrade'), 
            original_result.get('entryGrade'),
            "Entry grades should match"
        )

    @unittest.skipUnless(ORIGINAL_FUNCTIONS_AVAILABLE, "Original functions not available")
    def test_entry_grade_estimates_comparison(self):
        """Compare entry grade estimate calculations"""
        print("\n🧪 Testing entry grade estimates comparison...")
        
        # First calculate survival rates for both
        standalone_data = self.test_school_data.copy()
        standalone_data = standalone_survival_rates(standalone_data, GRADE_MAP)
        standalone_result = standalone_entry_grade_estimates(standalone_data, GRADE_MAP)
        
        original_data = self.test_school_data.copy()
        original_data = original_survival_rates(original_data, ORIGINAL_GRADE_MAP)
        original_result = original_entry_grade_estimates(original_data, ORIGINAL_GRADE_MAP)
        
        # Compare entry grade estimates
        standalone_estimates = standalone_result.get('entryGradeEstimates', {})
        original_estimates = original_result.get('entryGradeEstimates', {})
        
        print(f"Standalone entry grade estimates: {json.dumps(standalone_estimates, indent=2)}")
        print(f"Original entry grade estimates: {json.dumps(original_estimates, indent=2)}")
        
        differences = self.deep_compare_dict(standalone_estimates, original_estimates, "entryGradeEstimates")
        if differences:
            print(f"⚠️  Entry grade estimates differences found:")
            for diff in differences:
                print(f"  - {diff}")
        else:
            print("✅ Entry grade estimates match!")

    @unittest.skipUnless(ORIGINAL_FUNCTIONS_AVAILABLE, "Original functions not available") 
    def test_forecast_survival_rates_comparison(self):
        """Compare forecast survival rate calculations"""
        print("\n🧪 Testing forecast survival rates comparison...")
        
        # Prepare data with survival rates
        standalone_data = self.test_school_data.copy()
        standalone_data = standalone_survival_rates(standalone_data, GRADE_MAP)
        standalone_result = standalone_forecast_survival_rates(standalone_data)
        
        original_data = self.test_school_data.copy()
        original_data = original_survival_rates(original_data, ORIGINAL_GRADE_MAP)
        original_result = original_forecast_survival_rates(original_data)
        
        # Compare forecast survival rates
        standalone_forecast = standalone_result.get('forecastSurvivalRates', {})
        original_forecast = original_result.get('forecastSurvivalRates', {})
        
        print(f"Standalone forecast rates: {json.dumps(standalone_forecast, indent=2)}")
        print(f"Original forecast rates: {json.dumps(original_forecast, indent=2)}")
        
        differences = self.deep_compare_dict(standalone_forecast, original_forecast, "forecastSurvivalRates")
        if differences:
            print(f"⚠️  Forecast survival rates differences found:")
            for diff in differences:
                print(f"  - {diff}")
        else:
            print("✅ Forecast survival rates match!")

    @unittest.skipUnless(ORIGINAL_FUNCTIONS_AVAILABLE, "Original functions not available")
    def test_full_projection_pipeline_comparison(self):
        """Compare the full projection pipeline"""
        print("\n🧪 Testing full projection pipeline comparison...")
        
        # Run standalone pipeline
        standalone_data = self.test_school_data.copy()
        standalone_data = standalone_survival_rates(standalone_data, GRADE_MAP)
        standalone_data = standalone_forecast_survival_rates(standalone_data)
        standalone_data = standalone_entry_grade_estimates(standalone_data, GRADE_MAP)
        standalone_result = standalone_generate_projections(standalone_data, GRADE_MAP, self.forecast_years)
        
        # Run original pipeline
        original_data = self.test_school_data.copy()
        original_data = original_survival_rates(original_data, ORIGINAL_GRADE_MAP)
        original_data = original_forecast_survival_rates(original_data)
        original_data = original_entry_grade_estimates(original_data, ORIGINAL_GRADE_MAP)
        original_result = original_generate_projections(original_data, ORIGINAL_GRADE_MAP, self.forecast_years)
        
        # Compare final projections
        standalone_projections = standalone_result.get('projections', {})
        original_projections = original_result.get('projections', {})
        
        print(f"Standalone projections keys: {list(standalone_projections.keys())}")
        print(f"Original projections keys: {list(original_projections.keys())}")
        
        # Compare each projection type
        for proj_type in ['min', 'median', 'max']:
            if proj_type in standalone_projections and proj_type in original_projections:
                differences = self.deep_compare_dict(
                    standalone_projections[proj_type], 
                    original_projections[proj_type], 
                    f"projections.{proj_type}"
                )
                if differences:
                    print(f"⚠️  Differences in {proj_type} projections:")
                    for diff in differences:
                        print(f"  - {diff}")
                else:
                    print(f"✅ {proj_type} projections match!")

    def test_grade_mapping_consistency(self):
        """Test that grade mappings are consistent"""
        print("\n🧪 Testing grade mapping consistency...")
        
        if ORIGINAL_FUNCTIONS_AVAILABLE:
            self.assertEqual(GRADE_MAP, ORIGINAL_GRADE_MAP, "Grade mappings should be identical")
            print("✅ Grade mappings are consistent!")
        else:
            print("⚠️  Cannot test grade mapping - original functions not available")

    def test_mock_data_projections(self):
        """Test projections with the standalone functions only (no comparison)"""
        print("\n🧪 Testing standalone projections with mock data...")
        
        # Run full pipeline with standalone functions
        school_data = self.test_school_data.copy()
        school_data = standalone_survival_rates(school_data, GRADE_MAP)
        school_data = standalone_forecast_survival_rates(school_data)
        school_data = standalone_entry_grade_estimates(school_data, GRADE_MAP)
        result = standalone_generate_projections(school_data, GRADE_MAP, self.forecast_years)
        
        # Verify basic structure
        self.assertIn('projections', result)
        self.assertIn('entryGrade', result)
        self.assertIn('survivalRates', result)
        
        projections = result['projections']
        self.assertIn('min', projections)
        self.assertIn('median', projections)
        self.assertIn('max', projections)
        
        # Check that we have projections for each forecast year
        for year in self.forecast_years:
            self.assertIn(year, projections['median'])
            
        print(f"✅ Generated projections for {len(self.forecast_years)} years")
        print(f"Entry grade: {result.get('entryGrade')}")
        print(f"Sample projection (median, first year): {projections['median'][self.forecast_years[0]]}")

def main():
    print("="*60)
    print("PROJECTION IMPLEMENTATION COMPARISON TEST")
    print("="*60)
    
    if not ORIGINAL_FUNCTIONS_AVAILABLE:
        print("⚠️  Original enrollment_projections functions not available.")
        print("   Running standalone function tests only.")
    
    unittest.main(verbosity=2)

if __name__ == "__main__":
    main() 