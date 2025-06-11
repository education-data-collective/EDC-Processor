"""
Entity Processor

Main processing orchestrator for schools and location points.
Handles flexible processing pipelines based on entity type and data availability.
"""

from flask import current_app
from firebase_admin import firestore
from datetime import datetime
from models import (
    School, LocationPoint, SchoolLocation, ProcessingStatus,
    SchoolEnrollment, SchoolProjection, DistrictMetrics
)
from .utils import get_entity_info, determine_applicable_stages


class EntityProcessor:
    def __init__(self, entity_id, entity_type='school', data_year=None):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.data_year = data_year or datetime.now().year
        self.logger = current_app.logger
        
        # Get entity information
        self.entity_info = get_entity_info(entity_id, entity_type)
        if not self.entity_info:
            raise ValueError(f"{entity_type.title()} not found: {entity_id}")
        
        # Determine applicable processing stages
        self.stages = determine_applicable_stages(self.entity_info, entity_type)
        self.logger.info(f"Processing {entity_type} {entity_id} with stages: {self.stages}")

    async def process(self):
        """Main processing pipeline"""
        try:
            # Initialize Firebase document for tracking
            db = firestore.client()
            doc_id = f"{self.entity_type}_{self.entity_id}"
            status_ref = db.collection('entity_processing').document(doc_id)
            
            # Create initial processing status
            status_ref.set({
                'entity_id': self.entity_id,
                'entity_type': self.entity_type,
                'data_year': self.data_year,
                'started_at': firestore.SERVER_TIMESTAMP,
                'status': 'processing',
                'stages': {stage: {'status': 'pending'} for stage in self.stages}
            })

            # Process each applicable stage
            for stage in self.stages:
                try:
                    success = await self._process_stage(stage, status_ref)
                    if not success:
                        return False, f"Stage {stage} failed"
                except Exception as e:
                    self.logger.error(f"Stage {stage} error: {str(e)}")
                    status_ref.update({
                        f'stages.{stage}.status': 'failed',
                        f'stages.{stage}.error': str(e),
                        f'stages.{stage}.updated_at': firestore.SERVER_TIMESTAMP
                    })
                    return False, str(e)

            # Mark as completed
            status_ref.update({
                'status': 'completed',
                'completed_at': firestore.SERVER_TIMESTAMP
            })
            
            return True, None

        except Exception as e:
            self.logger.error(f"Processing error: {str(e)}")
            return False, str(e)

    async def _process_stage(self, stage, status_ref):
        """Process a single stage"""
        status_ref.update({
            f'stages.{stage}.status': 'in_progress',
            f'stages.{stage}.started_at': firestore.SERVER_TIMESTAMP
        })

        if stage == 'location':
            success = await self._process_location()
        elif stage == 'demographics':
            success = await self._process_demographics()
        elif stage == 'enrollment':
            success = await self._process_enrollment()
        elif stage == 'projections':
            success = await self._process_projections()
        elif stage == 'metrics':
            success = await self._process_metrics()
        else:
            raise ValueError(f"Unknown stage: {stage}")

        status = 'completed' if success else 'failed'
        status_ref.update({
            f'stages.{stage}.status': status,
            f'stages.{stage}.updated_at': firestore.SERVER_TIMESTAMP
        })

        return success

    async def _process_location(self):
        """Process location data"""
        from .task_handlers.location import process_location_data
        
        try:
            payload = {
                'entity_id': self.entity_id,
                'entity_type': self.entity_type,
                'data_year': self.data_year
            }
            result = await process_location_data(payload)
            return result.get('status') == 'success'
        except Exception as e:
            self.logger.error(f"Location processing error: {str(e)}")
            return False

    async def _process_demographics(self):
        """Process demographic data"""
        from .task_handlers.demographics import process_demographics
        
        try:
            # Get coordinates from entity
            if self.entity_type == 'school':
                location = self.entity_info['location']
                if not location:
                    return False
                coordinates = {
                    'latitude': location.location_point.latitude,
                    'longitude': location.location_point.longitude
                }
                location_id = location.location_point.id
            else:  # location point
                location_point = self.entity_info['entity']
                coordinates = {
                    'latitude': location_point.latitude,
                    'longitude': location_point.longitude
                }
                location_id = location_point.id
            
            payload = {
                'location_id': location_id,
                'coordinates': coordinates,
                'entity_id': self.entity_id,
                'entity_type': self.entity_type
            }
            result = await process_demographics(payload)
            return result.get('status') == 'success'
        except Exception as e:
            self.logger.error(f"Demographics processing error: {str(e)}")
            return False

    async def _process_enrollment(self):
        """Process enrollment data (schools only)"""
        if self.entity_type != 'school':
            return True  # N/A for location points
        
        from .task_handlers.enrollment import process_enrollment_data
        
        try:
            payload = {
                'school_id': self.entity_id,
                'data_year': self.data_year
            }
            result = await process_enrollment_data(payload)
            return result.get('status') == 'success'
        except Exception as e:
            self.logger.error(f"Enrollment processing error: {str(e)}")
            return False

    async def _process_projections(self):
        """Process projections (schools with enrollment only)"""
        if self.entity_type != 'school' or not self.entity_info['has_enrollment']:
            return True  # N/A for location points or schools without enrollment
        
        from .task_handlers.projections import process_projections
        
        try:
            payload = {
                'school_id': self.entity_id,
                'data_year': self.data_year
            }
            result = await process_projections(payload)
            return result.get('status') == 'success'
        except Exception as e:
            self.logger.error(f"Projections processing error: {str(e)}")
            return False

    async def _process_metrics(self):
        """Process district metrics"""
        from .task_handlers.metrics import process_metrics
        
        try:
            payload = {
                'entity_id': self.entity_id,
                'entity_type': self.entity_type,
                'data_year': self.data_year
            }
            result = await process_metrics(payload)
            return result.get('status') == 'success'
        except Exception as e:
            self.logger.error(f"Metrics processing error: {str(e)}")
            return False 