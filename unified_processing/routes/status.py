from .. import unified_bp
from flask import Blueprint, request, jsonify, current_app
from app.models import School, DirectoryEntry, EsriData
from app import db
from firebase_admin import firestore
from app.utils.auth_helpers import firebase_auth_required

@unified_bp.route('/processing-status/<string:ncessch>', methods=['GET'])
@firebase_auth_required()
async def get_processing_status(ncessch):
    """Get current processing status for a school"""
    try:
        current_app.logger.info(f"Checking processing status for {ncessch}")
        # Check if this is a split school
        is_split = '-' in ncessch
        parent_ncessch = ncessch.split('-')[0] if is_split else ncessch

        # Get school data
        school = School.query.filter_by(ncessch=ncessch).first()
        if not school:
            return jsonify({
                'status': 'error',
                'error': 'School not found'
            }), 404

        # Get directory entry
        dir_entry = DirectoryEntry.query.filter_by(
            school_id=school.id
        ).order_by(DirectoryEntry.data_year.desc()).first()

        # Get ESRI data status
        esri_data = EsriData.query.filter_by(
            ncessch=ncessch,
            has_data=1
        ).first()

        # Get Firestore data
        db = firestore.client()
        school_ref = db.collection('schools').document(ncessch)
        
        projections_exist = school_ref.collection('public_projections').document('current').get().exists
        metrics_exist = school_ref.collection('district_metrics').document('current').get().exists

        status = {
            'ncessch': ncessch,
            'school_name': school.school_name,
            'processing_type': 'split' if is_split else 'standard',
            'status': {
                'nces_data': 'completed' if dir_entry else 'missing',
                'esri_data': 'completed' if esri_data else 'missing',
                'projections': 'completed' if projections_exist else 'missing',
                'metrics': 'completed' if metrics_exist else 'missing'
            }
        }

        if is_split:
            parent_ref = db.collection('school_splits').document(parent_ncessch)
            parent_doc = parent_ref.get()
            if parent_doc.exists:
                split_data = parent_doc.to_dict()
                status['parent_school'] = parent_ncessch
                status['split_info'] = next(
                    (s for s in split_data['splits'] if s['ncessch'] == ncessch),
                    None
                )

        return jsonify(status)

    except Exception as e:
        current_app.logger.error(f"Status check error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500




