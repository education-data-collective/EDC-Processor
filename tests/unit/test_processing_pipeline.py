"""
Unit tests for the core processing pipeline.

These tests validate that mock data can flow through each processing stage
and that the orchestration logic works correctly with mocked dependencies.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

# Import the components we want to test
from entity_processing.processor import EntityProcessor
from entity_processing.task_handlers import (
    process_location_data,
    process_demographics,
    process_enrollment_data,
    process_projections,
    process_metrics
)


@pytest.mark.unit
class TestProcessingPipeline:
    """Test the complete processing pipeline with mocked dependencies"""
    
    @pytest.fixture
    def mock_processor(self):
        """Create EntityProcessor with mocked dependencies"""
        with patch('entity_processing.processor.firestore') as mock_firestore, \
             patch('entity_processing.processor.db') as mock_db:
            
            processor = EntityProcessor()
            processor.db = mock_db
            processor.firestore_client = mock_firestore
            return processor
    
    @pytest.fixture
    def sample_school_payload(self):
        """Sample school processing payload"""
        return {
            'entity_id': 'test-school-001',
            'entity_type': 'school',
            'data_year': 2024,
            'coordinates': {
                'latitude': 34.0522,
                'longitude': -118.2437
            },
            'location_id': 'test-location-001'
        }
    
    @pytest.fixture
    def sample_location_payload(self):
        """Sample location point processing payload"""
        return {
            'entity_id': 'test-location-001',
            'entity_type': 'location',
            'coordinates': {
                'latitude': 34.0522,
                'longitude': -118.2437
            },
            'location_id': 'test-location-001'
        }
    
    async def test_school_complete_pipeline(self, mock_processor, sample_school_payload):
        """Test that a school can go through all processing stages"""
        entity_id = sample_school_payload['entity_id']
        
        # Mock the determine_applicable_stages method
        with patch.object(mock_processor, 'determine_applicable_stages') as mock_stages:
            mock_stages.return_value = ['location', 'demographics', 'enrollment', 'projections', 'metrics']
            
            # Mock each stage processor
            with patch.object(mock_processor, 'process_location') as mock_location, \
                 patch.object(mock_processor, 'process_demographics') as mock_demographics, \
                 patch.object(mock_processor, 'process_enrollment') as mock_enrollment, \
                 patch.object(mock_processor, 'process_projections') as mock_projections, \
                 patch.object(mock_processor, 'process_metrics') as mock_metrics:
                
                # Configure mocks to return success
                mock_location.return_value = {'status': 'success', 'data': {'coordinates_validated': True}}
                mock_demographics.return_value = {'status': 'success', 'data': {'demographics_count': 2}}
                mock_enrollment.return_value = {'status': 'success', 'data': {'enrollment': 1245}}
                mock_projections.return_value = {'status': 'success', 'data': {'projections_created': 3}}
                mock_metrics.return_value = {'status': 'success', 'data': {'metrics_calculated': 5}}
                
                # Process the entity
                result = await mock_processor.process_entity(entity_id, 'school', 2024)
                
                # Verify all stages were called
                mock_location.assert_called_once()
                mock_demographics.assert_called_once()
                mock_enrollment.assert_called_once()
                mock_projections.assert_called_once()
                mock_metrics.assert_called_once()
                
                # Verify successful completion
                assert result['status'] == 'success'
                assert 'stages_completed' in result
    
    async def test_location_point_pipeline(self, mock_processor, sample_location_payload):
        """Test that a location point goes through appropriate stages only"""
        entity_id = sample_location_payload['entity_id']
        
        # Location points should only get location validation and demographics
        with patch.object(mock_processor, 'determine_applicable_stages') as mock_stages:
            mock_stages.return_value = ['location', 'demographics']
            
            with patch.object(mock_processor, 'process_location') as mock_location, \
                 patch.object(mock_processor, 'process_demographics') as mock_demographics, \
                 patch.object(mock_processor, 'process_enrollment') as mock_enrollment:
                
                mock_location.return_value = {'status': 'success', 'data': {'coordinates_validated': True}}
                mock_demographics.return_value = {'status': 'success', 'data': {'demographics_count': 2}}
                
                result = await mock_processor.process_entity(entity_id, 'location')
                
                # Verify only applicable stages were called
                mock_location.assert_called_once()
                mock_demographics.assert_called_once()
                mock_enrollment.assert_not_called()  # Should NOT be called for location points
                
                assert result['status'] == 'success'
    
    async def test_stage_failure_handling(self, mock_processor, sample_school_payload):
        """Test that pipeline handles stage failures correctly"""
        entity_id = sample_school_payload['entity_id']
        
        with patch.object(mock_processor, 'determine_applicable_stages') as mock_stages:
            mock_stages.return_value = ['location', 'demographics', 'enrollment']
            
            with patch.object(mock_processor, 'process_location') as mock_location, \
                 patch.object(mock_processor, 'process_demographics') as mock_demographics, \
                 patch.object(mock_processor, 'process_enrollment') as mock_enrollment:
                
                # Location succeeds, demographics fails, enrollment should not be called
                mock_location.return_value = {'status': 'success', 'data': {'coordinates_validated': True}}
                mock_demographics.return_value = {'status': 'error', 'error': 'ESRI API timeout'}
                
                result = await mock_processor.process_entity(entity_id, 'school', 2024)
                
                mock_location.assert_called_once()
                mock_demographics.assert_called_once() 
                mock_enrollment.assert_not_called()  # Should not proceed after failure
                
                assert result['status'] == 'error'
                assert 'demographics' in result.get('failed_stage', '')


@pytest.mark.unit
class TestIndividualTaskHandlers:
    """Test each task handler individually with mocked dependencies"""
    
    @pytest.mark.asyncio
    async def test_location_processing_success(self):
        """Test location processing task handler with valid data"""
        payload = {
            'entity_id': 'test-school-001',
            'entity_type': 'school',
            'data_year': 2024
        }
        
        # Mock the database and external services
        with patch('entity_processing.task_handlers.location.School') as mock_school_model, \
             patch('entity_processing.task_handlers.location.db') as mock_db, \
             patch('entity_processing.task_handlers.location.geocode_address') as mock_geocode:
            
            # Create mock school with location
            mock_school = Mock()
            mock_location_point = Mock()
            mock_location_point.latitude = 34.0522
            mock_location_point.longitude = -118.2437
            mock_location_point.address = '123 Test St'
            
            mock_school_location = Mock()
            mock_school_location.location_point = mock_location_point
            mock_school_location.is_current = True
            mock_school_location.data_year = 2024
            
            mock_school.locations = [mock_school_location]
            mock_school_model.query.get.return_value = mock_school
            
            # Test the location processing
            result = await process_location_data(payload)
            
            assert result['status'] == 'success'
            assert 'coordinates' in result['data']
            assert result['data']['coordinates']['latitude'] == 34.0522
    
    @pytest.mark.asyncio
    async def test_demographics_processing_success(self):
        """Test demographics processing with mocked ESRI data"""
        payload = {
            'location_id': 'test-location-001',
            'coordinates': {
                'latitude': 34.0522,
                'longitude': -118.2437
            },
            'entity_id': 'test-school-001',
            'entity_type': 'school'
        }
        
        # Mock database and ESRI service
        with patch('entity_processing.task_handlers.demographics.LocationPoint') as mock_location_model, \
             patch('entity_processing.task_handlers.demographics.EsriDemographicData') as mock_demo_model, \
             patch('entity_processing.task_handlers.demographics.db') as mock_db, \
             patch('entity_processing.task_handlers.demographics.fetch_esri_data_for_location') as mock_esri:
            
            # Mock location exists
            mock_location = Mock()
            mock_location_model.query.get.return_value = mock_location
            
            # Mock ESRI data response
            mock_esri.return_value = {
                5: {
                    'total_population': 1000,
                    'household_count': 400,
                    'median_household_income': 65000
                },
                10: {
                    'total_population': 2500,
                    'household_count': 950,
                    'median_household_income': 68000
                }
            }
            
            result = await process_demographics(payload)
            
            assert result['status'] == 'success'
            assert result['data']['stored_entries'] == 2
            mock_esri.assert_called_once_with(34.0522, -118.2437)
    
    @pytest.mark.asyncio
    async def test_enrollment_processing_success(self):
        """Test enrollment processing with mocked data"""
        payload = {
            'entity_id': 'test-school-001',
            'entity_type': 'school',
            'data_year': 2024
        }
        
        with patch('entity_processing.task_handlers.enrollment.School') as mock_school_model, \
             patch('entity_processing.task_handlers.enrollment.SchoolEnrollment') as mock_enrollment_model, \
             patch('entity_processing.task_handlers.enrollment.db') as mock_db:
            
            # Mock school exists
            mock_school = Mock()
            mock_school.id = 'test-school-001'
            mock_school_model.query.get.return_value = mock_school
            
            # Mock existing enrollment data
            mock_enrollment = Mock()
            mock_enrollment.enrollment_count = 1245
            mock_enrollment.data_year = 2024
            mock_enrollment_model.query.filter_by.return_value.first.return_value = mock_enrollment
            
            result = await process_enrollment_data(payload)
            
            assert result['status'] == 'success'
            assert 'enrollment_data' in result['data']
    
    @pytest.mark.asyncio 
    async def test_projections_processing_success(self):
        """Test projections processing with mocked data"""
        payload = {
            'entity_id': 'test-school-001',
            'entity_type': 'school',
            'data_year': 2024
        }
        
        with patch('entity_processing.task_handlers.projections.School') as mock_school_model, \
             patch('entity_processing.task_handlers.projections.SchoolEnrollment') as mock_enrollment_model, \
             patch('entity_processing.task_handlers.projections.SchoolProjection') as mock_projection_model, \
             patch('entity_processing.task_handlers.projections.db') as mock_db:
            
            # Mock school with enrollment data
            mock_school = Mock()
            mock_school_model.query.get.return_value = mock_school
            
            # Mock historical enrollment data for projections
            mock_enrollments = [
                Mock(enrollment_count=1200, data_year=2022),
                Mock(enrollment_count=1225, data_year=2023),
                Mock(enrollment_count=1245, data_year=2024)
            ]
            mock_enrollment_model.query.filter_by.return_value.order_by.return_value.all.return_value = mock_enrollments
            
            result = await process_projections(payload)
            
            assert result['status'] == 'success'
            assert 'projections_created' in result['data']
    
    @pytest.mark.asyncio
    async def test_metrics_processing_success(self):
        """Test district metrics processing"""
        payload = {
            'entity_id': 'test-school-001', 
            'entity_type': 'school',
            'data_year': 2024
        }
        
        with patch('entity_processing.task_handlers.metrics.School') as mock_school_model, \
             patch('entity_processing.task_handlers.metrics.DistrictMetrics') as mock_metrics_model, \
             patch('entity_processing.task_handlers.metrics.db') as mock_db:
            
            # Mock school with district relationship
            mock_school = Mock()
            mock_school.district_id = 'test-district-001'
            mock_school_model.query.get.return_value = mock_school
            
            # Mock district schools for metrics calculation
            mock_district_schools = [
                Mock(id='school-1', enrollment_count=1200),
                Mock(id='school-2', enrollment_count=800),
                Mock(id='test-school-001', enrollment_count=1245)
            ]
            mock_school_model.query.filter_by.return_value.all.return_value = mock_district_schools
            
            result = await process_metrics(payload)
            
            assert result['status'] == 'success'
            assert 'metrics_calculated' in result['data']


@pytest.mark.unit
class TestStageOrchestration:
    """Test stage determination and orchestration logic"""
    
    def test_school_stage_determination(self):
        """Test that schools get all applicable stages"""
        from entity_processing.processor import EntityProcessor
        
        processor = EntityProcessor()
        
        # Mock entity with all data available
        mock_entity_info = {
            'has_location': True,
            'has_enrollment': True,
            'has_district': True,
            'entity_type': 'school'
        }
        
        with patch.object(processor, 'get_entity_info') as mock_get_info:
            mock_get_info.return_value = mock_entity_info
            
            stages = processor.determine_applicable_stages('test-school-001', 'school')
            
            expected_stages = ['location', 'demographics', 'enrollment', 'projections', 'metrics']
            assert stages == expected_stages
    
    def test_location_point_stage_determination(self):
        """Test that location points get only location and demographics stages"""
        from entity_processing.processor import EntityProcessor
        
        processor = EntityProcessor()
        
        mock_entity_info = {
            'has_location': True,
            'has_enrollment': False,  # Location points don't have enrollment
            'has_district': False,    # Location points don't have districts
            'entity_type': 'location'
        }
        
        with patch.object(processor, 'get_entity_info') as mock_get_info:
            mock_get_info.return_value = mock_entity_info
            
            stages = processor.determine_applicable_stages('test-location-001', 'location')
            
            expected_stages = ['location', 'demographics']  # Only these two
            assert stages == expected_stages
    
    def test_school_without_enrollment_stage_determination(self):
        """Test that schools without enrollment data skip enrollment-dependent stages"""
        from entity_processing.processor import EntityProcessor
        
        processor = EntityProcessor()
        
        mock_entity_info = {
            'has_location': True,
            'has_enrollment': False,  # No enrollment data
            'has_district': True,
            'entity_type': 'school'
        }
        
        with patch.object(processor, 'get_entity_info') as mock_get_info:
            mock_get_info.return_value = mock_entity_info
            
            stages = processor.determine_applicable_stages('test-school-001', 'school')
            
            # Should skip projections (requires enrollment) but still do metrics
            expected_stages = ['location', 'demographics', 'metrics']
            assert stages == expected_stages 