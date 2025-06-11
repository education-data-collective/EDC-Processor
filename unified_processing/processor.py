from flask import Blueprint, request, jsonify, current_app
from sqlalchemy import or_, text
from sqlalchemy.orm import scoped_session, sessionmaker
from app import db
from firebase_admin import firestore
from app.models import School, DirectoryEntry
from app.services.district_metrics.calculator import calculate_district_metrics
from app.utils.geocoding import GeocodingService
import logging
from enrollment_projections.main import generate_and_update_projections
from .task_handlers import (
    process_esri_data,
)
from .utils import get_db_session, success, error

unified_bp = Blueprint('unified', __name__)

class UnifiedProcessor:
    def __init__(self, ncessch, processing_type='standard'):
        self.ncessch = ncessch
        self.processing_type = processing_type
        self.logger = current_app.logger
        self.status = {
            'steps': {
                'validation': 'pending',
                'nces_data': 'pending',
                'esri_data': 'pending',
                'polygons': 'pending',
                'projections': 'pending',
                'metrics': 'pending'
            },
            'errors': {},
            'splits': []
        }

    async def process(self):
        """Main processing pipeline"""
        try:
            # Initialize Firebase document
            db = firestore.client()
            school_ref = db.collection('schools').document(self.ncessch)
            
            # Create initial processing status
            school_ref.set({
                'processing_status': {
                    'started_at': firestore.SERVER_TIMESTAMP,
                    'status': 'processing',
                    'type': self.processing_type,
                    'stages': {
                        'validation': {'status': 'pending'},
                        'nces_data': {'status': 'pending'},
                        'esri_data': {'status': 'pending'},
                        'projections': {'status': 'pending'},
                        'metrics': {'status': 'pending'}
                    }
                }
            }, merge=True)

            # Step 1: Initial validation
            if not await self.validate_school():
                return False, "School validation failed"

            # Step 2: Process NCES data if split school
            if self.processing_type == 'split':
                if not await self.process_split_school():
                    return False, "Split processing failed"

            # Step 3: Process ESRI data and polygons
            if not await self.process_esri_data():
                return False, "ESRI processing failed"

            # Step 4: Generate projections
            if not await self.generate_projections():
                return False, "Projections generation failed"

            # Step 5: Calculate district metrics
            if not await self.calculate_district_metrics():
                return False, "District metrics calculation failed"

            # Step 6: Update team assignments
            if not await self.update_teams():
                return False, "Team assignment failed"

            if success:
                # Update completion status after all steps succeed
                school_ref.update({
                    'processing_status.completed_at': firestore.SERVER_TIMESTAMP,
                    'processing_status.status': 'completed'
                })
                return True, None
            else:
                # Update failed status if any step failed
                school_ref.update({
                    'processing_status.completed_at': firestore.SERVER_TIMESTAMP,
                    'processing_status.status': 'failed',
                    'processing_status.error': error
                })
                return False, error

        except Exception as e:
            # Update error status if exception occurred
            school_ref.update({
                'processing_status.completed_at': firestore.SERVER_TIMESTAMP,
                'processing_status.status': 'failed',
                'processing_status.error': str(e)
            })
            self.logger.error(f"Processing error: {str(e)}")
            return False, str(e)

    async def validate_school(self):
        """Validate school and determine processing type"""
        try:
            school = School.query.filter_by(ncessch=self.ncessch).first()
            if not school:
                self.status['errors']['validation'] = 'School not found'
                return False

            # Check for existing splits
            existing_splits = School.query.filter(or_(
                School.ncessch == f"{self.ncessch}-es",
                School.ncessch == f"{self.ncessch}-ms",
                School.ncessch == f"{self.ncessch}-hs"
            )).all()

            # Validate split configuration if this is a split request
            if self.processing_type == 'split':
                if not hasattr(self, 'splits_configuration'):
                    self.status['errors']['validation'] = 'Split configuration required'
                    return False

                # Validate each split
                for split in self.splits_configuration:
                    if not all(k in split for k in ['ncessch', 'displayName', 'gradesServed', 'address']):
                        self.status['errors']['validation'] = 'Invalid split configuration'
                        return False

            self.status['steps']['validation'] = 'completed'
            return True

        except Exception as e:
            self.status['steps']['validation'] = 'failed'
            self.status['errors']['validation'] = str(e)
            return False

    async def process_split_school(self):
        """Process a split school"""
        try:
            # Update Firebase with split info
            db = firestore.client()
            school_ref = db.collection('schools').document(self.ncessch)
            
            split_components = []
            for split_config in self.splits_configuration:
                split_components.append({
                    'ncessch': split_config['ncessch'],
                    'type': split_config['type'],
                    'displayName': split_config['displayName'],
                    'status': 'pending'
                })
            
            school_ref.update({
                'processing_status.split_components': split_components
            })
            
            parent_school = School.query.filter_by(ncessch=self.ncessch).first()
            if not parent_school:
                return False

            # Clean up existing splits first
            await self.cleanup_existing_splits()

            # Create new splits
            for split_config in self.splits_configuration:
                # Geocode address
                if split_config.get('address'):
                    geocoding_service = GeocodingService()
                    coordinates = geocoding_service.geocode_address(
                        split_config['address'].get('street'),
                        split_config['address'].get('city'),
                        split_config['address'].get('state'),
                        split_config['address'].get('zip')
                    )
                    if coordinates:
                        split_config['address'].update(coordinates)

                success = await self.create_split(parent_school, split_config)
                if not success:
                    return False

            self.status['steps']['nces_data'] = 'completed'
            return True

        except Exception as e:
            self.status['steps']['nces_data'] = 'failed'
            self.status['errors']['nces_data'] = str(e)
            return False
        
    async def process_esri_data(self):
        """Process ESRI data for the school"""
        esri_session = None
        try:
            # Get ESRI engine and session
            esri_session = get_db_session('esri_data')
            
            # Get school coordinates from main database
            school = School.query.filter_by(ncessch=self.ncessch).first()
            if not school:
                raise ValueError(f"School not found: {self.ncessch}")

            dir_entry = DirectoryEntry.query.filter_by(
                school_id=school.id
            ).order_by(DirectoryEntry.data_year.desc()).first()

            if not dir_entry or not dir_entry.latitude or not dir_entry.longitude:
                raise ValueError("No coordinates available for school")

            # Process ESRI data
            result = await process_esri_data({
                'ncessch': self.ncessch,
                'coordinates': {
                    'latitude': dir_entry.latitude,
                    'longitude': dir_entry.longitude
                }
            }, esri_session)

            if result.get('status') == 'error':
                raise ValueError(result.get('error'))

            self.status['steps']['esri_data'] = 'completed'
            return True

        except Exception as e:
            self.status['steps']['esri_data'] = 'failed'
            self.status['errors']['esri_data'] = str(e)
            return False
        finally:
            if esri_session:
                esri_session.remove()
    
    async def generate_projections(self):
        """Generate enrollment projections"""
        try:
            schools_to_process = [self.ncessch]
            if self.processing_type == 'split':
                schools_to_process.extend(s['ncessch'] for s in self.status['splits'])

            db = firestore.client()
            for ncessch in schools_to_process:
                school_ref = db.collection('schools').document(ncessch)
                
                # Generate projections using existing method
                projections = generate_and_update_projections(ncessch, None)
                if not projections:
                    raise ValueError(f"Failed to generate projections for {ncessch}")

                # Save projections to Firestore
                proj_ref = school_ref.collection('public_projections').document('current')
                proj_ref.set(projections)

            self.status['steps']['projections'] = 'completed'
            return True

        except Exception as e:
            self.status['steps']['projections'] = 'failed'
            self.status['errors']['projections'] = str(e)
            return False

    async def calculate_district_metrics(self):
        """Calculate district metrics"""
        try:
            schools_to_process = [self.ncessch]
            if self.processing_type == 'split':
                schools_to_process.extend(s['ncessch'] for s in self.status['splits'])

            # Get database sessions
            nces_session = get_db_session('nces_data')
            esri_session = get_db_session('esri_data')
            firestore_db = firestore.client()

            # Get school objects for processing
            schools = School.query.filter(School.ncessch.in_(schools_to_process)).all()

            # Calculate metrics using existing method
            success = await calculate_district_metrics(
                nces_session,
                esri_session,
                firestore_db,
                schools
            )

            if not success:
                raise ValueError("Failed to calculate district metrics")

            self.status['steps']['metrics'] = 'completed'
            return True

        except Exception as e:
            self.status['steps']['metrics'] = 'failed'
            self.status['errors']['metrics'] = str(e)
            return False