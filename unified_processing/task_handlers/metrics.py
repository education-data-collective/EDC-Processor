from flask import current_app
from sqlalchemy import create_engine, text, extract
from sqlalchemy.orm import scoped_session, sessionmaker
import os
from app import db
from app.models import School, EsriData
from firebase_admin import firestore
from datetime import datetime
from app.services.district_metrics.calculator import calculate_district_metrics
from app.unified_processing.utils import update_overall_status


def get_database_url(database_name, db_port):
    """Helper function to construct database URL"""
    url = f"postgresql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST', 'localhost')}:{db_port}/{database_name}"
    current_app.logger.info(f"Constructed URL for {database_name} (password masked)")
    return url

def get_db_engine(bind_key):
    """Get the correct database engine based on environment"""
    try:
        # First try the bind parameter
        return db.get_engine(bind=bind_key)
    except TypeError:
        try:
            return db.engines[bind_key]
        except (KeyError, AttributeError):
            if hasattr(db, '_engines'):
                return db._engines.get(bind_key)
            return db.engine

def verify_database_connections(nces_engine, esri_engine):
    """Verify database connections are working"""
    try:
        # Test NCES connection
        with nces_engine.connect() as conn:
            result = conn.execute(text("SELECT current_database()"))
            current_app.logger.info(f"Connected to NCES database: {result.scalar()}")
        
        # Test ESRI connection
        with esri_engine.connect() as conn:
            result = conn.execute(text("SELECT current_database()"))
            current_app.logger.info(f"Connected to ESRI database: {result.scalar()}")
            
        return True
    except Exception as e:
        current_app.logger.error(f"Database verification failed: {str(e)}")
        return False

async def process_metrics(payload):
    """Process district metrics for school(s)"""
    nces_session = None
    esri_session = None
    try:
        current_app.logger.info("Starting metrics processing")
        
        # Create database engines
        try:
            nces_engine = get_db_engine('nces_data')
            esri_engine = get_db_engine('esri_data')
            
            # Verify database connections
            if not verify_database_connections(nces_engine, esri_engine):
                raise ValueError("Database verification failed")

            # Create sessions
            nces_session = scoped_session(sessionmaker(bind=nces_engine))
            esri_session = scoped_session(sessionmaker(bind=esri_engine))
            firestore_db = firestore.client()

            # Process schools
            schools_to_process = []
            if ncessch := payload.get('ncessch'):
                schools_to_process.append(ncessch)
            elif parent_ncessch := payload.get('parent_ncessch'):
                schools_to_process.extend(payload.get('splits', []))
            else:
                raise ValueError("No valid school identifiers provided")

            current_app.logger.info(f"Processing metrics for schools: {schools_to_process}")
            results = {'processed': [], 'failed': []}

            for school_ncessch in schools_to_process:
                status_ref = None
                try:
                    # Get status reference
                    status_ref = (firestore_db.collection('schools')
                                .document(school_ncessch)
                                .collection('processing_status')
                                .document('current'))
                    
                    # Update in-progress status
                    status_ref.update({
                        'stages.metrics': {
                            'status': 'in_progress',
                            'updated_at': firestore.SERVER_TIMESTAMP
                        }
                    })

                    # Get school with ORM
                    school = nces_session.query(School).filter_by(ncessch=school_ncessch).first()
                    if not school:
                        raise ValueError(f"School not found: {school_ncessch}")

                    # Verify ESRI data exists and is current
                    current_app.logger.info(f"Checking ESRI data for {school_ncessch}")

                    # Query all ESRI data first for debugging
                    all_esri = esri_session.query(EsriData).filter(
                        EsriData.ncessch == school_ncessch,
                        EsriData.has_data == 1
                    ).all()

                    current_app.logger.info(f"Found {len(all_esri)} total ESRI records")
                    for record in all_esri:
                        current_app.logger.info(f"ESRI record - Drive time: {record.drive_time}, Timestamp: {record.timestamp}")

                    # Now try to get the most recent one
                    esri_data = esri_session.query(EsriData).filter(
                        EsriData.ncessch == school_ncessch,
                        EsriData.has_data == 1
                    ).order_by(EsriData.timestamp.desc()).first()

                    if not esri_data:
                        current_app.logger.error(f"No ESRI data found for {school_ncessch}")
                        raise ValueError(f"No ESRI data found for {school_ncessch}")

                    current_app.logger.info(f"Using ESRI data from {esri_data.timestamp} (drive time: {esri_data.drive_time})")

                    if not esri_data:
                        raise ValueError(f"No current ESRI data found for {school_ncessch}")

                    # Calculate metrics - this function is async and writes to database
                    success = await calculate_district_metrics(
                        nces_session,
                        esri_session,
                        firestore_db,
                        [school]
                    )

                    if not success:
                        raise ValueError("Failed to calculate district metrics")

                    # Update success status
                    status_ref.update({
                        'stages.metrics': {
                            'status': 'completed',
                            'updated_at': firestore.SERVER_TIMESTAMP,
                            'details': 'Metrics calculated successfully'
                        }
                    })

                    status_doc = status_ref.get()
                    if status_doc.exists:
                        current_data = status_doc.to_dict()
                        current_stages = current_data.get('stages', {})
                        update_overall_status(status_ref, current_stages)

                    results['processed'].append({
                        'ncessch': school_ncessch,
                        'status': 'success'
                    })

                except Exception as e:
                    current_app.logger.error(f"Error processing metrics for {school_ncessch}: {str(e)}")
                    results['failed'].append({
                        'ncessch': school_ncessch,
                        'error': str(e)
                    })
                    
                    # Update error status
                    if status_ref:
                        try:
                            status_ref.update({
                                'stages.metrics': {
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

            # Final check of results
            if not results['processed']:
                raise ValueError("No schools were successfully processed")

            return {
                'status': 'success',
                'results': results
            }

        except Exception as e:
            current_app.logger.error(f"Database operation error: {str(e)}")
            if nces_session:
                nces_session.rollback()
            if esri_session:
                esri_session.rollback()
            raise

    except Exception as e:
        current_app.logger.error(f"Metrics processing error: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }

    finally:
        # Clean up sessions
        if nces_session:
            nces_session.remove()
        if esri_session:
            esri_session.remove()