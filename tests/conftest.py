"""
Pytest configuration and fixtures for EDC Processor tests

Provides both unit test fixtures (with mocks) and integration test fixtures 
(with real Cloud SQL and Firebase connections).
"""

import pytest
import os
import asyncio
from unittest.mock import Mock, patch
from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import firebase_admin
from firebase_admin import credentials, firestore

# Import our application components
from models import Base


# =============================================================================
# TEST CONFIGURATION
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests (fast, mocked)"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (slower, real services)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )


# =============================================================================
# UNIT TEST FIXTURES (MOCKED)
# =============================================================================

@pytest.fixture
def mock_app():
    """Create a Flask app with mocked dependencies for unit tests"""
    app = Flask(__name__)
    app.config.update({
        'TESTING': True,
        'SECRET_KEY': 'test-secret-key',
        'DATABASE_URL': 'sqlite:///:memory:',
        'FIREBASE_PROJECT_ID': 'test-project',
    })
    
    # Mock database
    with patch('models.db') as mock_db:
        mock_db.session = Mock()
        mock_db.session.commit = Mock()
        mock_db.session.rollback = Mock()
        mock_db.session.add = Mock()
        mock_db.session.query = Mock()
        
        with app.app_context():
            yield app


@pytest.fixture
def mock_firebase():
    """Mock Firebase for unit tests"""
    with patch('firebase_admin.firestore.client') as mock_client:
        mock_doc = Mock()
        mock_doc.set = Mock()
        mock_doc.update = Mock()
        mock_doc.get = Mock()
        
        mock_collection = Mock()
        mock_collection.document.return_value = mock_doc
        mock_collection.add = Mock()
        
        mock_client.return_value.collection.return_value = mock_collection
        yield mock_client


# =============================================================================
# INTEGRATION TEST FIXTURES (REAL SERVICES)
# =============================================================================

@pytest.fixture(scope="session")
def integration_app():
    """Create Flask app with real database connection for integration tests"""
    app = Flask(__name__)
    
    # Override with test database URL
    test_db_url = os.getenv('TEST_DATABASE_URL')
    if not test_db_url:
        pytest.skip("TEST_DATABASE_URL not configured - skipping integration tests")
    
    app.config.update({
        'TESTING': True,
        'DATABASE_URL': test_db_url,
        'SQLALCHEMY_DATABASE_URI': test_db_url,
    })
    
    return app


@pytest.fixture(scope="session")
def db_engine():
    """Create database engine for integration tests"""
    test_db_url = os.getenv('TEST_DATABASE_URL')
    if not test_db_url:
        pytest.skip("TEST_DATABASE_URL not configured")
    
    engine = create_engine(test_db_url)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")  
def db_session(db_engine):
    """Create database session for integration tests"""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    
    # Create tables if they don't exist
    Base.metadata.create_all(db_engine)
    
    yield session
    
    # Cleanup
    session.close()


@pytest.fixture
def db_transaction(db_session):
    """Provide a database transaction that rolls back after each test"""
    transaction = db_session.begin()
    yield db_session
    transaction.rollback()


@pytest.fixture(scope="session")
def firebase_client():
    """Create Firebase client for integration tests"""
    test_project_id = os.getenv('TEST_FIREBASE_PROJECT_ID')
    test_credentials_path = os.getenv('TEST_FIREBASE_SERVICE_ACCOUNT_PATH')
    
    if not test_project_id:
        pytest.skip("TEST_FIREBASE_PROJECT_ID not configured")
    
    if not test_credentials_path or not os.path.exists(test_credentials_path):
        pytest.skip("TEST_FIREBASE_SERVICE_ACCOUNT_PATH not configured or file not found")
    
    # Initialize Firebase app for testing
    cred = credentials.Certificate(test_credentials_path)
    app = firebase_admin.initialize_app(cred, {
        'projectId': test_project_id
    }, name='test-app')
    
    client = firestore.client(app)
    yield client
    
    # Cleanup
    firebase_admin.delete_app(app)


# =============================================================================
# TEST DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_school_data():
    """Sample school data for testing"""
    return {
        'id': 'test-school-001',
        'name': 'Test Elementary School',
        'address': '123 Main St',
        'city': 'Test City',
        'state': 'CA',
        'zip_code': '90210',
        'latitude': 34.0522,
        'longitude': -118.2437,
        'data_year': 2024
    }


@pytest.fixture
def sample_location_data():
    """Sample location point data for testing"""
    return {
        'id': 'test-location-001',
        'name': 'Test Location Point',
        'address': '456 Oak Ave',
        'city': 'Test City',
        'state': 'CA', 
        'zip_code': '90210',
        'latitude': 34.0622,
        'longitude': -118.2537
    }


# =============================================================================
# ASYNC TEST SUPPORT
# =============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =============================================================================
# CLEANUP FIXTURES
# =============================================================================

@pytest.fixture
def cleanup_test_data(request):
    """Cleanup test data after integration tests (only when fixture is used)"""
    yield
    
    # Only run cleanup for integration tests
    if 'integration' in request.keywords and os.getenv('CLEANUP_TEST_DATA', 'True').lower() == 'true':
        try:
            # This would need a real db_session to work
            # For now, this is just a placeholder
            pass
        except Exception:
            pass


@pytest.fixture
def firebase_cleanup(firebase_client):
    """Cleanup Firebase test documents"""
    test_docs = []
    
    def track_doc(collection, doc_id):
        """Track a document for cleanup"""
        test_docs.append((collection, doc_id))
    
    yield track_doc
    
    # Cleanup tracked documents
    for collection, doc_id in test_docs:
        try:
            firebase_client.collection(collection).document(doc_id).delete()
        except Exception:
            pass  # Ignore cleanup errors 