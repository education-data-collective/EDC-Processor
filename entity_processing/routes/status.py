"""
Entity Processing Status Routes

API endpoints for monitoring processing status and progress.
"""

from flask import request, jsonify, current_app
from firebase_admin import firestore
from .. import entity_bp
from ..utils import success_response, error_response
from ..task_handlers.location import get_location_summary
from ..task_handlers.demographics import get_demographic_summary
from ..task_handlers.enrollment import get_enrollment_summary
from ..task_handlers.projections import get_projections_summary
from ..task_handlers.metrics import get_metrics_summary


@entity_bp.route('/status/<int:entity_id>', methods=['GET'])
def get_entity_status(entity_id):
    """Get processing status for an entity"""
    try:
        entity_type = request.args.get('entity_type', 'school')
        
        current_app.logger.info(f"Getting status for {entity_type} {entity_id}")
        
        # Get Firebase processing status
        db = firestore.client()
        doc_id = f"{entity_type}_{entity_id}"
        status_doc = db.collection('entity_processing').document(doc_id).get()
        
        firebase_status = status_doc.to_dict() if status_doc.exists else None
        
        # Get detailed status from database
        location_status = get_location_summary(entity_id, entity_type)
        
        status_data = {
            'entity_id': entity_id,
            'entity_type': entity_type,
            'firebase_status': firebase_status,
            'data_status': {
                'location': location_status
            }
        }
        
        # Add stage-specific status for schools
        if entity_type == 'school':
            status_data['data_status'].update({
                'enrollment': get_enrollment_summary(entity_id),
                'projections': get_projections_summary(entity_id),
                'metrics': get_metrics_summary(entity_id)
            })
        
        # Add demographics status if location available
        if location_status and location_status.get('location_id'):
            status_data['data_status']['demographics'] = get_demographic_summary(
                location_status['location_id']
            )
        
        return jsonify(success_response(status_data, "Status retrieved successfully"))
        
    except Exception as e:
        current_app.logger.error(f"Status error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/status/batch', methods=['POST'])
def get_batch_status():
    """Get processing status for multiple entities"""
    try:
        data = request.get_json()
        entity_ids = data.get('entity_ids', [])
        entity_type = data.get('entity_type', 'school')
        
        if not entity_ids:
            return jsonify(error_response("entity_ids list is required")), 400
        
        results = {}
        
        for entity_id in entity_ids:
            try:
                # Get Firebase processing status
                db = firestore.client()
                doc_id = f"{entity_type}_{entity_id}"
                status_doc = db.collection('entity_processing').document(doc_id).get()
                
                firebase_status = status_doc.to_dict() if status_doc.exists else None
                
                # Get basic location status
                location_status = get_location_summary(entity_id, entity_type)
                
                results[str(entity_id)] = {
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'firebase_status': firebase_status,
                    'has_location': bool(location_status and location_status.get('has_coordinates'))
                }
                
                # Add enrollment status for schools
                if entity_type == 'school':
                    enrollment_status = get_enrollment_summary(entity_id)
                    results[str(entity_id)]['has_enrollment'] = bool(
                        enrollment_status and enrollment_status.get('has_enrollment')
                    )
                
            except Exception as e:
                current_app.logger.error(f"Error getting status for {entity_type} {entity_id}: {str(e)}")
                results[str(entity_id)] = {
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'error': str(e)
                }
        
        return jsonify(success_response(results, f"Batch status retrieved for {len(entity_ids)} entities"))
        
    except Exception as e:
        current_app.logger.error(f"Batch status error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/status/processing', methods=['GET'])
def get_active_processing():
    """Get list of entities currently being processed"""
    try:
        db = firestore.client()
        
        # Query for documents with status 'processing'
        processing_docs = db.collection('entity_processing').where(
            'status', '==', 'processing'
        ).get()
        
        active_processing = []
        for doc in processing_docs:
            data = doc.to_dict()
            active_processing.append({
                'document_id': doc.id,
                'entity_id': data.get('entity_id'),
                'entity_type': data.get('entity_type'),
                'started_at': data.get('started_at'),
                'stages': data.get('stages', {}),
                'data_year': data.get('data_year')
            })
        
        return jsonify(success_response({
            'active_processing': active_processing,
            'count': len(active_processing)
        }, f"Found {len(active_processing)} entities currently processing"))
        
    except Exception as e:
        current_app.logger.error(f"Active processing query error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/status/summary', methods=['GET'])
def get_processing_summary():
    """Get overall processing statistics"""
    try:
        db = firestore.client()
        
        # Get counts by status
        all_docs = db.collection('entity_processing').get()
        
        summary = {
            'total_processed': len(all_docs),
            'by_status': {
                'processing': 0,
                'completed': 0,
                'failed': 0
            },
            'by_entity_type': {
                'school': 0,
                'location': 0
            },
            'recent_activity': []
        }
        
        recent_docs = []
        
        for doc in all_docs:
            data = doc.to_dict()
            status = data.get('status', 'unknown')
            entity_type = data.get('entity_type', 'unknown')
            
            if status in summary['by_status']:
                summary['by_status'][status] += 1
            
            if entity_type in summary['by_entity_type']:
                summary['by_entity_type'][entity_type] += 1
            
            # Collect recent activity
            started_at = data.get('started_at')
            if started_at:
                recent_docs.append({
                    'entity_id': data.get('entity_id'),
                    'entity_type': entity_type,
                    'status': status,
                    'started_at': started_at
                })
        
        # Sort by started_at and take most recent
        recent_docs.sort(key=lambda x: x['started_at'], reverse=True)
        summary['recent_activity'] = recent_docs[:10]  # Last 10 activities
        
        return jsonify(success_response(summary, "Processing summary retrieved"))
        
    except Exception as e:
        current_app.logger.error(f"Processing summary error: {str(e)}")
        return jsonify(error_response(str(e))), 500 