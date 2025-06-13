"""
Integration tests for database operations

These tests use the real Cloud SQL test database to validate:
- Database schema and table creation
- Model relationships and constraints
- Complex queries and data operations
- Transaction handling
"""

import pytest
from datetime import datetime
from models import (
    School, LocationPoint, SchoolLocation, 
    ProcessingStatus, EsriDemographicData
)


@pytest.mark.integration
class TestDatabaseSchema:
    """Test database schema and basic operations"""
    
    def test_tables_exist(self, db_session):
        """Test that all required tables exist in the database"""
        # Test basic table existence by querying metadata
        tables = db_session.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchall()
        
        table_names = [table[0] for table in tables]
        
        # Check for key tables
        expected_tables = [
            'schools', 'location_points', 'school_locations',
            'processing_status', 'esri_demographic_data'
        ]
        
        for table in expected_tables:
            assert table in table_names, f"Table {table} not found in database"
    
    def test_create_school_with_location(self, db_transaction):
        """Test creating a school with associated location"""
        # Create location point
        location = LocationPoint(
            id='test-location-001',
            latitude=34.0522,
            longitude=-118.2437,
            address='123 Test St',
            city='Los Angeles',
            state='CA',
            zip_code='90210'
        )
        db_transaction.add(location)
        
        # Create school
        school = School(
            id='test-school-001',
            name='Integration Test School'
        )
        db_transaction.add(school)
        
        # Create school location relationship
        school_location = SchoolLocation(
            school_id='test-school-001',
            location_point_id='test-location-001',
            is_current=True,
            data_year=2024
        )
        db_transaction.add(school_location)
        
        db_transaction.commit()
        
        # Verify the data was saved
        saved_school = db_transaction.query(School).filter_by(id='test-school-001').first()
        saved_location = db_transaction.query(LocationPoint).filter_by(id='test-location-001').first()
        
        assert saved_school is not None
        assert saved_school.name == 'Integration Test School'
        assert saved_location is not None
        assert saved_location.latitude == 34.0522
    
    def test_processing_status_tracking(self, db_transaction):
        """Test processing status tracking functionality"""
        # Create processing status record
        status = ProcessingStatus(
            entity_id='test-school-002',
            entity_type='school',
            stage='location',
            status='in_progress',
            data_year=2024,
            started_at=datetime.utcnow()
        )
        db_transaction.add(status)
        db_transaction.commit()
        
        # Update status
        status.status = 'completed'
        status.completed_at = datetime.utcnow()
        db_transaction.commit()
        
        # Verify update
        saved_status = db_transaction.query(ProcessingStatus).filter_by(
            entity_id='test-school-002'
        ).first()
        
        assert saved_status.status == 'completed'
        assert saved_status.completed_at is not None


@pytest.mark.integration  
class TestComplexQueries:
    """Test complex database queries and relationships"""
    
    def test_school_location_relationships(self, db_transaction, sample_school_data):
        """Test querying schools with their locations"""
        # Setup test data
        location = LocationPoint(
            id='test-location-complex-001',
            latitude=sample_school_data['latitude'],
            longitude=sample_school_data['longitude'],
            address=sample_school_data['address'],
            city=sample_school_data['city'],
            state=sample_school_data['state'],
            zip_code=sample_school_data['zip_code']
        )
        db_transaction.add(location)
        
        school = School(
            id=sample_school_data['id'],
            name=sample_school_data['name']
        )
        db_transaction.add(school)
        
        school_location = SchoolLocation(
            school_id=sample_school_data['id'],
            location_point_id='test-location-complex-001',
            is_current=True,
            data_year=sample_school_data['data_year']
        )
        db_transaction.add(school_location)
        db_transaction.commit()
        
        # Test complex query with joins
        result = db_transaction.query(School, LocationPoint).join(
            SchoolLocation, School.id == SchoolLocation.school_id
        ).join(
            LocationPoint, SchoolLocation.location_point_id == LocationPoint.id
        ).filter(
            School.id == sample_school_data['id']
        ).first()
        
        assert result is not None
        school_obj, location_obj = result
        assert school_obj.name == sample_school_data['name']
        assert location_obj.latitude == sample_school_data['latitude']
    
    def test_demographic_data_storage(self, db_transaction):
        """Test storing and querying demographic data"""
        # Create location for demographic data
        location = LocationPoint(
            id='test-location-demo-001',
            latitude=34.0522,
            longitude=-118.2437,
            address='456 Demo St',
            city='Los Angeles',
            state='CA',
            zip_code='90210'
        )
        db_transaction.add(location)
        
        # Create demographic data for different drive times
        demo_5min = EsriDemographicData(
            location_id='test-location-demo-001',
            drive_time=5,
            total_population=1000,
            household_count=400,
            median_household_income=65000,
            timestamp=datetime.utcnow()
        )
        
        demo_10min = EsriDemographicData(
            location_id='test-location-demo-001', 
            drive_time=10,
            total_population=2500,
            household_count=950,
            median_household_income=68000,
            timestamp=datetime.utcnow()
        )
        
        db_transaction.add_all([demo_5min, demo_10min])
        db_transaction.commit()
        
        # Query demographic data
        demographics = db_transaction.query(EsriDemographicData).filter_by(
            location_id='test-location-demo-001'
        ).order_by(EsriDemographicData.drive_time).all()
        
        assert len(demographics) == 2
        assert demographics[0].drive_time == 5
        assert demographics[0].total_population == 1000
        assert demographics[1].drive_time == 10
        assert demographics[1].total_population == 2500


@pytest.mark.integration
class TestTransactionHandling:
    """Test database transaction handling and rollback scenarios"""
    
    def test_transaction_rollback_on_error(self, db_transaction):
        """Test that transactions roll back properly on errors"""
        initial_count = db_transaction.query(LocationPoint).count()
        
        try:
            # Add a valid location
            location1 = LocationPoint(
                id='test-location-rollback-001',
                latitude=34.0522,
                longitude=-118.2437,
                address='123 Rollback St',
                city='Test City',
                state='CA',
                zip_code='90210'
            )
            db_transaction.add(location1)
            
            # Try to add an invalid location (this should cause an error)
            location2 = LocationPoint(
                id='test-location-rollback-002',
                latitude=999.0,  # Invalid latitude
                longitude=-118.2437,
                address='456 Invalid St',
                city='Test City',
                state='CA',
                zip_code='90210'
            )
            db_transaction.add(location2)
            
            # This commit should fail due to constraint violations
            db_transaction.commit()
            
            # If we get here, the test should fail
            assert False, "Expected constraint violation did not occur"
            
        except Exception:
            # Expected - transaction should rollback
            db_transaction.rollback()
        
        # Verify no data was committed
        final_count = db_transaction.query(LocationPoint).count()
        assert final_count == initial_count, "Transaction was not properly rolled back"
    
    def test_concurrent_status_updates(self, db_transaction):
        """Test handling concurrent status updates"""
        # Create initial processing status
        status = ProcessingStatus(
            entity_id='test-concurrent-001',
            entity_type='school',
            stage='location',
            status='in_progress',
            data_year=2024,
            started_at=datetime.utcnow()
        )
        db_transaction.add(status)
        db_transaction.commit()
        
        # Simulate concurrent update
        status_copy = db_transaction.query(ProcessingStatus).filter_by(
            entity_id='test-concurrent-001'
        ).first()
        
        status_copy.status = 'completed'
        status_copy.completed_at = datetime.utcnow()
        
        db_transaction.commit()
        
        # Verify the update was successful
        final_status = db_transaction.query(ProcessingStatus).filter_by(
            entity_id='test-concurrent-001'
        ).first()
        
        assert final_status.status == 'completed'
        assert final_status.completed_at is not None 