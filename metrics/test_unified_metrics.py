#!/usr/bin/env python3
"""
Test Script for Unified Metrics Calculator

This script tests the unified metrics calculator with the edc_unified database.
It validates data flow, calculations, and database operations.
"""

import os
import sys
import unittest
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add metrics module to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from metrics.unified_calculator import (
        create_database_connection,
        fetch_school_data,
        fetch_enrollment_data,
        fetch_esri_data,
        get_school_projections_from_database,
        calculate_metrics_for_school,
        calculate_unified_metrics,
        stop_cloud_sql_proxy,
        proxy_process
    )
    from metrics.utils import (
        calculate_grade_filtered_population,
        calculate_enrollment,
        get_school_grades,
        calculate_market_share,
        calculate_percent_change,
        get_status,
        validate_ncessch
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running this from the project root directory")
    sys.exit(1)

class TestUnifiedMetrics(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection (once for all tests)"""
        logging.basicConfig(level=logging.INFO)
        cls.logger = logging.getLogger(__name__)
        
        try:
            cls.engine = create_database_connection()
            cls.logger.info("✅ Database connection established for testing")
        except Exception as e:
            cls.logger.error(f"❌ Failed to connect to database: {str(e)}")
            raise
    
    @classmethod
    def tearDownClass(cls):
        """Clean up database connection"""
        global proxy_process
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)
            cls.logger.info("Database connection cleaned up")
    
    def test_database_connection(self):
        """Test basic database connectivity"""
        print("\n🧪 Testing database connection...")
        
        self.assertIsNotNone(self.engine)
        
        # Test basic query
        with self.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT COUNT(*) FROM schools")).scalar()
            self.assertGreater(result, 0, "Should have schools in database")
            print(f"✅ Found {result:,} schools in database")
    
    def test_fetch_school_data(self):
        """Test school data fetching"""
        print("\n🧪 Testing school data fetching...")
        
        # Fetch a small sample
        schools = fetch_school_data(self.engine, limit=10)
        
        self.assertIsInstance(schools, list)
        self.assertGreater(len(schools), 0, "Should fetch at least some schools")
        
        # Validate school data structure
        for school in schools[:3]:  # Check first 3
            required_fields = ['school_id', 'ncessch', 'school_name', 'location_id']
            for field in required_fields:
                self.assertIn(field, school, f"School missing field: {field}")
                self.assertIsNotNone(school[field], f"School field {field} is None")
            
            # Validate NCESSCH format
            self.assertIsInstance(school['ncessch'], str)
            self.assertGreater(len(school['ncessch']), 0)
            
        print(f"✅ Successfully fetched {len(schools)} schools")
        print(f"   Sample school: {schools[0]['school_name']} ({schools[0]['ncessch']})")
    
    def test_fetch_enrollment_data(self):
        """Test enrollment data fetching"""
        print("\n🧪 Testing enrollment data fetching...")
        
        # Get a few schools first
        schools = fetch_school_data(self.engine, limit=5)
        school_ids = [school['school_id'] for school in schools]
        
        enrollment_data = fetch_enrollment_data(self.engine, school_ids)
        
        self.assertIsInstance(enrollment_data, dict)
        
        # Check data structure
        if enrollment_data:
            sample_school_id = list(enrollment_data.keys())[0]
            sample_enrollment = enrollment_data[sample_school_id]
            
            self.assertIn('current', sample_enrollment)
            self.assertIn('comparison', sample_enrollment)
            
            print(f"✅ Fetched enrollment data for {len(enrollment_data)} schools")
            
            # Show sample data
            if sample_enrollment['current']:
                grades = list(sample_enrollment['current'].keys())[:3]
                print(f"   Sample grades: {grades}")
        else:
            print("⚠️  No enrollment data found for sample schools")
    
    def test_fetch_esri_data(self):
        """Test ESRI demographic data fetching"""
        print("\n🧪 Testing ESRI data fetching...")
        
        # Get schools with location IDs
        schools = fetch_school_data(self.engine, limit=5)
        location_ids = [school['location_id'] for school in schools]
        
        esri_data = fetch_esri_data(self.engine, location_ids)
        
        self.assertIsInstance(esri_data, dict)
        
        if esri_data:
            sample_location_id = list(esri_data.keys())[0]
            sample_esri = esri_data[sample_location_id]
            
            # Check structure
            self.assertIn('ages', sample_esri)
            self.assertIn('4_17', sample_esri['ages'])
            
            age_data = sample_esri['ages']['4_17']
            required_periods = ['current', 'future', '2020']
            for period in required_periods:
                self.assertIn(period, age_data)
                self.assertIsInstance(age_data[period], list)
                self.assertEqual(len(age_data[period]), 14, f"Should have 14 age groups for {period}")
            
            print(f"✅ Fetched ESRI data for {len(esri_data)} locations")
            print(f"   Sample ages 5-17 current: {sum(age_data['current'][1:])}")
        else:
            print("⚠️  No ESRI data found for sample locations")
    
    def test_utility_functions(self):
        """Test utility functions from utils.py"""
        print("\n🧪 Testing utility functions...")
        
        # Test NCESSCH validation
        valid_ncessch = "123456789012"
        result = validate_ncessch(valid_ncessch)
        self.assertEqual(result, valid_ncessch)
        
        # Test grade filtering and population calculation
        mock_esri_data = {
            'ages': {
                '4_17': {
                    'current': [10, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27],  # ages 4-17
                    'future': [12, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
                    '2020': [8, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
                }
            }
        }
        
        selected_grades = ['Kindergarten', '1', '2', '3', '4', '5']
        pop_totals = calculate_grade_filtered_population(mock_esri_data, selected_grades)
        
        self.assertIn('current', pop_totals)
        self.assertIn('future', pop_totals)
        self.assertIn('past', pop_totals)
        
        # Should sum ages 5-10 (K-5 corresponds to ages 5-10)
        expected_current = sum(mock_esri_data['ages']['4_17']['current'][1:7])  # ages 5-10
        self.assertEqual(pop_totals['current'], expected_current)
        
        # Test enrollment calculation
        enrollment_data = {
            'Kindergarten': 25,
            'Grade 1': 24,
            'Grade 2': 23,
            'Grade 3': 22,
            'Grade 4': 21,
            'Grade 5': 20
        }
        
        total_enrollment = calculate_enrollment(enrollment_data, selected_grades)
        expected_total = 25 + 24 + 23 + 22 + 21 + 20  # 135
        self.assertEqual(total_enrollment, expected_total)
        
        # Test market share calculation
        market_share = calculate_market_share(total_enrollment, pop_totals['current'])
        self.assertGreater(market_share, 0)
        # Note: Market share can be > 100% for private schools that draw from wider areas
        
        print("✅ Utility functions working correctly")
    
    def test_metrics_calculation_pipeline(self):
        """Test the complete metrics calculation pipeline"""
        print("\n🧪 Testing metrics calculation pipeline...")
        
        # Get a single school for detailed testing
        schools = fetch_school_data(self.engine, limit=1)
        if not schools:
            self.skipTest("No schools available for testing")
        
        school = schools[0]
        
        # Fetch associated data
        enrollment_data = fetch_enrollment_data(self.engine, [school['school_id']])
        esri_data = fetch_esri_data(self.engine, [school['location_id']])
        projections = get_school_projections_from_database(self.engine, school['ncessch'])
        
        # Calculate metrics
        metrics = calculate_metrics_for_school(school, enrollment_data, esri_data, projections)
        
        if metrics:
            # Validate metrics structure
            required_fields = [
                'ncessch', 'calculated_at',
                'population_current', 'population_past', 'population_future',
                'enrollment_current', 'enrollment_past',
                'market_share_current', 'market_share_past',
                'population_trend_status', 'enrollment_trend_status'
            ]
            
            for field in required_fields:
                self.assertIn(field, metrics, f"Missing field: {field}")
            
            # Validate data types and ranges
            self.assertIsInstance(metrics['population_current'], int)
            self.assertGreaterEqual(metrics['population_current'], 0)
            
            self.assertIsInstance(metrics['enrollment_current'], int)
            self.assertGreaterEqual(metrics['enrollment_current'], 0)
            
            self.assertIsInstance(metrics['market_share_current'], (int, float))
            self.assertGreaterEqual(metrics['market_share_current'], 0)
            
            # Status fields should be valid strings
            valid_statuses = ['growing', 'declining', 'stable', 'gaining', 'losing']
            self.assertIn(metrics['population_trend_status'], valid_statuses)
            self.assertIn(metrics['enrollment_trend_status'], valid_statuses)
            
            print(f"✅ Metrics calculated successfully for {school['school_name']}")
            print(f"   Current enrollment: {metrics['enrollment_current']}")
            print(f"   Market share: {metrics['market_share_current']:.2f}%")
            print(f"   Population trend: {metrics['population_trend_status']}")
        else:
            print("⚠️  No metrics calculated (likely missing data)")
    
    def test_end_to_end_small_batch(self):
        """Test end-to-end processing with a small batch"""
        print("\n🧪 Testing end-to-end processing (small batch)...")
        
        try:
            # Run with a small limit
            results = calculate_unified_metrics(limit=5)
            
            self.assertIsInstance(results, dict)
            self.assertIn('total', results)
            self.assertIn('success', results)
            self.assertIn('errors', results)
            
            self.assertGreaterEqual(results['total'], 0)
            self.assertGreaterEqual(results['success'], 0)
            self.assertGreaterEqual(results['errors'], 0)
            
            print(f"✅ End-to-end test completed")
            print(f"   Total schools: {results['total']}")
            print(f"   Successful: {results['success']}")
            print(f"   Errors: {results['errors']}")
            
            if results['success'] > 0:
                # Verify data was saved to database
                with self.engine.connect() as conn:
                    from sqlalchemy import text
                    count = conn.execute(text("SELECT COUNT(*) FROM district_metrics")).scalar()
                    self.assertGreater(count, 0, "Should have saved metrics to database")
                    print(f"   Metrics saved to database: {count}")
            
        except Exception as e:
            self.fail(f"End-to-end test failed: {str(e)}")
    
    def test_data_validation(self):
        """Test data validation and error handling"""
        print("\n🧪 Testing data validation...")
        
        # Test with invalid NCESSCH
        with self.assertRaises(ValueError):
            validate_ncessch("")
        
        with self.assertRaises(ValueError):
            validate_ncessch("x" * 20)  # Too long
        
        # Test with empty data
        empty_enrollment = {}
        empty_grades = get_school_grades({'enrollment_by_grade': {'current': empty_enrollment}})
        self.assertEqual(empty_grades, [])
        
        # Test with malformed data
        bad_esri_data = {}
        result = calculate_grade_filtered_population(bad_esri_data, ['Kindergarten'])
        expected = {'past': 0, 'current': 0, 'future': 0}
        self.assertEqual(result, expected)
        
        print("✅ Data validation working correctly")

def run_database_checks():
    """Run some basic database checks before tests"""
    print("🔍 Running pre-test database checks...")
    
    try:
        engine = create_database_connection()
        
        with engine.connect() as conn:
            from sqlalchemy import text
            
            # Check critical tables
            tables_to_check = [
                'schools', 'school_directory', 'school_names', 'school_locations',
                'location_points', 'school_enrollments', 'esri_demographic_data',
                'district_metrics'
            ]
            
            for table in tables_to_check:
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    print(f"   {table}: {count:,} records")
                    
                    if table in ['schools', 'school_directory'] and count == 0:
                        print(f"⚠️  Warning: {table} is empty - tests may fail")
                        
                except Exception as e:
                    print(f"❌ Error checking {table}: {str(e)}")
        
        # Clean up
        global proxy_process
        if proxy_process:
            stop_cloud_sql_proxy(proxy_process)
            
        print("✅ Database checks completed")
        return True
        
    except Exception as e:
        print(f"❌ Database check failed: {str(e)}")
        return False

def main():
    """Main test runner"""
    print("="*60)
    print("UNIFIED METRICS CALCULATOR TESTS")
    print("="*60)
    
    # Run database checks first
    if not run_database_checks():
        print("❌ Database checks failed - aborting tests")
        return 1
    
    print("\n🧪 Running unit tests...")
    
    # Configure test runner
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestUnifiedMetrics)
    
    runner = unittest.TextTestRunner(
        verbosity=2,
        descriptions=True,
        failfast=False
    )
    
    result = runner.run(suite)
    
    print("\n" + "="*60)
    if result.wasSuccessful():
        print("✅ All tests passed!")
        return 0
    else:
        print(f"❌ Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 