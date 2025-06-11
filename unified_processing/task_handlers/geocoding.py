from flask import current_app
from app.utils.geocoding import GeocodingService
from firebase_admin import firestore
from app.unified_processing.utils import update_overall_status
import json

def process_geocoding(payload):
    """Process geocoding for a split school component"""
    try:
        current_app.logger.info(f"Starting process_geocoding with payload: {json.dumps(payload, default=str)}")
        
        ncessch = payload.get('ncessch')
        parent_ncessch = payload.get('parent_ncessch')
        address = payload.get('address')
        
        current_app.logger.info(f"Extracted fields - ncessch: {ncessch}, parent_ncessch: {parent_ncessch}, address: {json.dumps(address, default=str)}")
        
        if not all([ncessch, parent_ncessch, address]):
            current_app.logger.error(f"Missing required fields. ncessch: {bool(ncessch)}, parent_ncessch: {bool(parent_ncessch)}, address: {bool(address)}")
            raise ValueError("Missing required payload data")

        # Initialize Firebase refs
        db = firestore.client()
        status_ref = (db.collection('schools')
                     .document(ncessch)
                     .collection('processing_status')
                     .document('current'))
        splits_ref = db.collection('school_splits').document(parent_ncessch)
        
        current_app.logger.info(f"Initialized Firebase refs for ncessch: {ncessch}")
        
        # Update status to in_progress
        status_ref.update({
            'stages.geocoding': {
                'status': 'in_progress',
                'updated_at': firestore.SERVER_TIMESTAMP
            }
        })

        current_app.logger.info(f"Processing geocoding for split school {ncessch}")
        
        # Initialize geocoding service and process address
        geocoding_service = GeocodingService()
        
        coordinates = geocoding_service.geocode_address(
            address.get('street'),
            address.get('city'),
            address.get('state'),
            address.get('zip')
        )
        current_app.logger.info(f"Geocoding result for {ncessch}: {coordinates}")

        if not coordinates:
            current_app.logger.error(f"Geocoding failed for {ncessch} - no coordinates returned")
            raise ValueError("Geocoding failed - no coordinates returned")

        # Update address with coordinates
        current_app.logger.info(f"Pre-update address structure: {json.dumps(address, default=str)}")
        address.update(coordinates)
        current_app.logger.info(f"Post-update address structure: {json.dumps(address, default=str)}")

        # Store in school_splits collection
        split_doc = splits_ref.get()
        if split_doc.exists:
            splits_data = split_doc.to_dict()
            splits = splits_data.get('splits', [])
            
            current_app.logger.info(f"Retrieved splits data: {json.dumps(splits_data, default=str)}")
            
            # Update the matching split's address
            for split in splits:
                if split.get('ncessch') == ncessch:
                    current_app.logger.info(f"Found matching split. Pre-update: {json.dumps(split, default=str)}")
                    split['address'] = address
                    current_app.logger.info(f"Post-update split: {json.dumps(split, default=str)}")
                    break

            # Update Firestore
            splits_ref.update({
                'splits': splits,
                'last_updated': firestore.SERVER_TIMESTAMP
            })

        current_app.logger.info(f"Completed Firestore updates for {ncessch}")

        # Update success status for this split
        status_ref.update({
            'stages.geocoding': {
                'status': 'completed',
                'updated_at': firestore.SERVER_TIMESTAMP,
                'details': f"Successfully geocoded {ncessch}"
            }
        })

        # Changed return structure to match what task_manager expects
        current_app.logger.info(f"Returning success response for {ncessch}")
        return {
            'name': 'local_task',  # Added to match local task structure
            'result': coordinates  # Changed to put coordinates directly in result
        }

    except Exception as e:
        current_app.logger.error(f"Geocoding error: {str(e)}")
        try:
            # Update error status on split's document
            status_ref.update({
                'stages.geocoding': {
                    'status': 'failed',
                    'error': str(e),
                    'updated_at': firestore.SERVER_TIMESTAMP
                }
            })
            # Update overall status
            status_doc = status_ref.get()
            if status_doc.exists:
                current_data = status_doc.to_dict()
                current_stages = current_data.get('stages', {})
                update_overall_status(status_ref, current_stages)
        except Exception as firebase_error:
            current_app.logger.error(f"Failed to update Firebase status: {str(firebase_error)}")
        raise