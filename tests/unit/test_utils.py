"""
Unit tests for entity_processing.utils module.
"""

import pytest
from unittest.mock import Mock, patch
from entity_processing.utils import (
    success_response,
    error_response,
    validate_entity
)


class TestResponseHelpers:
    """Test response helper functions."""
    
    def test_success_response_with_data(self):
        """Test success response creation with data."""
        data = {'test': 'value'}
        message = 'Success message'
        
        response = success_response(data, message)
        
        assert response['status'] == 'success'
        assert response['data'] == data
        assert response['message'] == message
    
    def test_success_response_without_data(self):
        """Test success response creation without data."""
        message = 'Success message'
        
        response = success_response(message=message)
        
        assert response['status'] == 'success'
        assert response['message'] == message
        assert 'data' not in response
    
    def test_error_response_basic(self):
        """Test error response creation."""
        error = 'Test error'
        
        response = error_response(error)
        
        assert response['status'] == 'error'
        assert response['error'] == error
        assert 'details' not in response
    
    def test_error_response_with_details(self):
        """Test error response with details."""
        error = 'Test error'
        details = {'code': 400, 'field': 'entity_id'}
        
        response = error_response(error, details)
        
        assert response['status'] == 'error'
        assert response['error'] == error
        assert response['details'] == details


class TestEntityValidation:
    """Test entity validation functions."""
    
    @patch('entity_processing.utils.get_entity_info')
    def test_validate_entity_valid_school(self, mock_get_info):
        """Test validation of valid school entity."""
        mock_get_info.return_value = {
            'entity': Mock(),
            'location': Mock(),
            'has_location': True,
            'has_enrollment': True,
            'name': 'Test School'
        }
        
        result = validate_entity(123, 'school')
        
        assert result['valid'] is True
        assert 'entity_info' in result
        assert 'applicable_stages' in result
    
    @patch('entity_processing.utils.get_entity_info')
    def test_validate_entity_not_found(self, mock_get_info):
        """Test validation of non-existent entity."""
        mock_get_info.return_value = None
        
        result = validate_entity(999, 'school')
        
        assert result['valid'] is False
        assert 'School not found' in result['error']
    
    @patch('entity_processing.utils.get_entity_info')
    def test_validate_entity_no_location(self, mock_get_info):
        """Test validation of entity without location."""
        mock_get_info.return_value = {
            'entity': Mock(),
            'location': None,
            'has_location': False,
            'has_enrollment': False,
            'name': 'Test School'
        }
        
        result = validate_entity(123, 'school')
        
        assert result['valid'] is False
        assert 'No valid coordinates' in result['error'] 