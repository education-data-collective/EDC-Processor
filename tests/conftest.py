"""
Pytest configuration and fixtures for EDC Processor tests.
"""

import pytest
from flask import Flask
from unittest.mock import Mock
import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def app():
    """Create application for testing."""
    app = Flask(__name__)
    app.config['TESTING'] = True
    app.config['DATABASE_URL'] = 'sqlite:///:memory:'
    app.config['WTF_CSRF_ENABLED'] = False
    return app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def mock_db():
    """Mock database session."""
    return Mock()


@pytest.fixture
def mock_firebase():
    """Mock Firebase client."""
    return Mock()


@pytest.fixture
def sample_school_data():
    """Sample school data for testing."""
    return {
        'id': 1,
        'name': 'Test School',
        'nces_id': '123456789',
        'state_id': 'TS001'
    }


@pytest.fixture
def sample_location_data():
    """Sample location data for testing."""
    return {
        'id': 1,
        'latitude': 40.7128,
        'longitude': -74.0060,
        'address': '123 Test St',
        'city': 'Test City',
        'state': 'NY',
        'zip_code': '12345'
    }


@pytest.fixture
def sample_processing_payload():
    """Sample processing payload for testing."""
    return {
        'entity_id': 1,
        'entity_type': 'school',
        'data_year': 2024
    } 