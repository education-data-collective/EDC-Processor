from flask import current_app
from app import db
from app.esri.fetch import fetch_esri_data
from app.models import School, DirectoryEntry, EsriData
from datetime import datetime
from firebase_admin import firestore
from sqlalchemy import extract
from app.unified_processing.utils import update_overall_status
from app.esri.process import update_polygon_relationships


def process_esri_data(payload, session=None):
    """Process ESRI data fetching and nearby schools"""
    try:
        # Use the correct bind for ESRI data operations
        if not session:
            session = db.session
            
        ncessch = payload.get('ncessch')
        coordinates = payload.get('coordinates')
        
        if not ncessch or not coordinates:
            raise ValueError("Missing required payload data")

        # Initialize Firebase status
        firestore_db = firestore.client()
        status_ref = (firestore_db.collection('schools')
                     .document(ncessch)
                     .collection('processing_status')
                     .document('current'))
                     
        status_ref.update({
            'stages.esri_data': {
                'status': 'in_progress',
                'updated_at': firestore.SERVER_TIMESTAMP
            }
        })


        # Verify coordinates
        latitude = coordinates.get('latitude')
        longitude = coordinates.get('longitude')
        if not latitude or not longitude:
            raise ValueError("Invalid coordinates provided")

        # Fetch ESRI data
        current_app.logger.info(f"Fetching ESRI data for {ncessch}")
        esri_data = fetch_esri_data(latitude, longitude)
        if not esri_data:
            raise ValueError("Failed to fetch ESRI data")

        stored_entries = 0
        try:
            for drive_time, data in esri_data.items():
                # Prepare fields for database
                esri_fields = {
                    k: v for k, v in data.items() 
                    if hasattr(EsriData, k) and k != 'id'
                }
                
                esri_fields.update({
                    'ncessch': ncessch,
                    'latitude': latitude,
                    'longitude': longitude,
                    'drive_time': drive_time,
                    'timestamp': datetime.utcnow(),
                    'has_data': 1
                })

                # Check for existing record
                existing = EsriData.query.filter(
                    EsriData.ncessch == ncessch,
                    EsriData.drive_time == drive_time,
                    extract('year', EsriData.timestamp) == datetime.utcnow().year
                ).first()

                if existing:
                    # Update existing record
                    for key, value in esri_fields.items():
                        setattr(existing, key, value)
                else:
                    # Create new record
                    new_record = EsriData(**esri_fields)
                    session.add(new_record)

                stored_entries += 1

            # Commit the transaction
            session.commit()

            # Process polygon relationships with the same ESRI data
            current_app.logger.info(f"Processing polygon relationships for {ncessch}")
            polygon_success = update_polygon_relationships(ncessch, esri_data)
            
            if not polygon_success:
                current_app.logger.warning(f"Failed to update polygon relationships for {ncessch}")
                # Note: We're not failing the entire process if just the polygon update fails
                # But we do log it as a warning

            # Update success status in Firebase with polygon processing info
            status_details = f'Processed {stored_entries} drive time records'
            if polygon_success is False:  # Only add this if specifically False (not None)
                status_details += ', polygon relationships failed'
            
            status_ref.update({
                'stages.esri_data': {
                    'status': 'completed',
                    'updated_at': firestore.SERVER_TIMESTAMP,
                    'details': status_details
                }
            })

            # Update overall status
            status_doc = status_ref.get()
            if status_doc.exists:
                current_data = status_doc.to_dict()
                current_stages = current_data.get('stages', {})
                update_overall_status(status_ref, current_stages)

            return {
                'status': 'success',
                'ncessch': ncessch,
                'stored_entries': stored_entries
            }

        except Exception as e:
            session.rollback()
            raise

    except Exception as e:
        current_app.logger.error(f"ESRI processing error: {str(e)}")
        if ncessch:
            try:
                status_ref = (firestore_db.collection('schools')
                            .document(ncessch)
                            .collection('processing_status')
                            .document('current'))
                status_ref.update({
                    'stages.esri_data': {
                        'status': 'failed',
                        'error': str(e),
                        'updated_at': firestore.SERVER_TIMESTAMP
                    }
                })
                # Get current stages and update overall status even on failure
                status_doc = status_ref.get()
                if status_doc.exists:
                    current_data = status_doc.to_dict()
                    current_stages = current_data.get('stages', {})
                    update_overall_status(status_ref, current_stages)
            except Exception as firebase_error:
                current_app.logger.error(f"Failed to update Firebase status: {str(firebase_error)}")
        return {
            'status': 'error',
            'error': str(e)
        }