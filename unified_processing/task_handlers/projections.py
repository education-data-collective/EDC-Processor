from flask import current_app
from firebase_admin import firestore
from app.models import School, MembershipData
from enrollment_projections.main import generate_and_update_projections
import logging
from app.unified_processing.utils import update_overall_status


def process_projections(payload):
    """Process public enrollment projections for school(s)"""
    try:
        # Handle both single school and split school cases
        ncessch = payload.get('ncessch')
        parent_ncessch = payload.get('parent_ncessch')
        splits = payload.get('splits', [])

        schools_to_process = []
        if ncessch:
            schools_to_process.append(ncessch)
        elif parent_ncessch and splits:
            schools_to_process = splits
        else:
            raise ValueError("No valid school identifiers provided")

        current_app.logger.info(f"Processing public projections for schools: {schools_to_process}")

        results = {
            'processed': [],
            'failed': []
        }

        db = firestore.client()

        for school_ncessch in schools_to_process:
            try:
                # Get both status_ref and school_ref
                school_ref = db.collection('schools').document(school_ncessch)
                status_ref = school_ref.collection('processing_status').document('current')
                
                # Update status to in_progress
                status_ref.update({
                    'stages.projections': {
                        'status': 'in_progress',
                        'updated_at': firestore.SERVER_TIMESTAMP
                    }
                })

                current_app.logger.info(f"Generating public projections for {school_ncessch}")
                
                # Get school from database
                school = School.query.filter_by(ncessch=school_ncessch).first()
                if not school:
                    raise ValueError(f"School not found: {school_ncessch}")

                # Get historical enrollment data
                enrollment_data = {}
                membership_rows = MembershipData.query.filter_by(
                    school_id=school.id
                ).order_by(MembershipData.data_year).all()

                for row in membership_rows:
                    year = row.data_year
                    if year not in enrollment_data:
                        enrollment_data[year] = {}
                    enrollment_data[year][row.grade] = row.total_membership

                # Generate public projections using NCES data
                projections = generate_and_update_projections(
                    school_ncessch,
                    enrollment_data
                )

                if not projections:
                    raise ValueError("Failed to generate projections")

                # Store public projections
                public_proj_ref = school_ref.collection('public_projections').document('current')
                public_proj_ref.set({
                    **projections,
                    'updated_at': firestore.SERVER_TIMESTAMP,
                    'source': 'automatic',
                    'type': 'public'
                })

                # Update success status
                status_ref.update({
                    'stages.projections': {
                        'status': 'completed',
                        'updated_at': firestore.SERVER_TIMESTAMP,
                        'details': 'Public projections generated and stored successfully'
                    }
                })

                # Update overall status
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
                current_app.logger.error(f"Error processing projections for {school_ncessch}: {str(e)}")
                # Update error status
                try:
                    status_ref.update({
                        'stages.projections': {
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

                results['failed'].append({
                    'ncessch': school_ncessch,
                    'error': str(e)
                })

        if not results['processed']:
            raise ValueError("No schools were successfully processed")

        return {
            'status': 'success',
            'results': results
        }

    except Exception as e:
        current_app.logger.error(f"Projections processing error: {str(e)}")
        raise