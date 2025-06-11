from .. import unified_bp
from flask import Blueprint, request, jsonify, current_app
from ..processor import UnifiedProcessor
from ..task_manager import TaskManager
from app.models import School, DirectoryEntry, EsriData
from app import db
from sqlalchemy import or_
from firebase_admin import firestore
from app.esri.process import delete_esri_data
from app.utils.school_utils import validate_split_configuration, transform_split_data
from app.utils.auth_helpers import admin_required, firebase_auth_required
from app.unified_processing.utils import validate_schools_structure, validate_single_school
from app.unified_processing.task_handlers.team import process_team_assignment

# Route to process a single school
@unified_bp.route('/process', methods=['POST'])
@firebase_auth_required()
@admin_required
def process_school():
    """Process a single school - handles both standard and split processing"""
    try:
        data = request.get_json()
        ncessch = data.get('ncessch')
        process_type = data.get('type', 'standard')
        team_id = data.get('team_id')
        user_id = request.user['uid']

        current_app.logger.info(f"Starting school processing: {ncessch} (type: {process_type})")

        # Validate school first
        validation_result = validate_single_school(ncessch)
        if validation_result.get('status') == 'error':
            return jsonify(validation_result), 400

        # Initialize Firebase document
        db = firestore.client()
        status_ref = (db.collection('schools')
                     .document(ncessch)
                     .collection('processing_status')
                     .document('current'))
        
        # Define stages with appropriate initial status
        stages = {
            'validation': {'status': 'completed'},  # Change to completed since we just validated
            'nces_data': {'status': 'not_applicable' if process_type == 'standard' else 'pending'},
            'esri_data': {'status': 'pending'},
            'projections': {'status': 'pending'},
            'metrics': {'status': 'pending'}
        }
        
        status_data = {
            'started_at': firestore.SERVER_TIMESTAMP,
            'type': process_type,
            'overall_status': 'processing',
            'stages': stages
        }
        
        status_ref.set(status_data)

        # Create task manager
        task_manager = TaskManager()
        
        # Create processing tasks based on type
        if process_type == 'split':
            result = task_manager.process_split_school({
                'ncessch': ncessch,
                'type': process_type,
                'team_id': team_id
            })
        else:
            result = task_manager.process_standard_school({
                'ncessch': ncessch,
                'type': process_type,
                'team_id': team_id
            })

        return jsonify({
            'status': 'success',
            'message': 'Processing tasks created',
            'tasks': result['tasks']
        })

    except Exception as e:
        current_app.logger.error(f"Processing error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Route for processing schools in bulk
@unified_bp.route('/process/bulk', methods=['POST'])
@firebase_auth_required()
@admin_required
def bulk_process():
    current_app.logger.info("Starting bulk processing")
    try:
        data = request.get_json()
        current_app.logger.info(f"Received data: {data}")
        
        # Handle both direct schools data and validation result format
        schools_data = data.get('schools', [])
        validation_result = data.get('validationResult', {})
        if validation_result and validation_result.get('needs_splitting'):
            schools_data.extend(validation_result['needs_splitting'])
            
        current_app.logger.info(f"Extracted schools_data: {schools_data}")
        process_type = data.get('type', 'standard')
        force_proceed = data.get('force_proceed', False)

        # Run validation first
        validation_results = validate_schools_structure(schools_data)
        if 'ready_for_processing' not in validation_results['data']:
            validation_results['data']['ready_for_processing'] = []

        current_app.logger.info(f"Bulk processing request: {len(schools_data)} schools (type: {process_type})")

        # Process each school
        for school in schools_data:
            if process_type == 'split':
                # Split school processing
                ncessch = school.get('parent_ncessch') or school.get('ncessch')
                
                # Basic school existence check
                parent = School.query.filter_by(ncessch=ncessch).first()
                if not parent:
                    validation_results['critical_issues'].append({
                        'ncessch': ncessch,
                        'issue': 'School not found'
                    })
                    continue

                # Get the latest directory entry with coordinates
                latest_entry = (DirectoryEntry.query
                                .filter_by(school_id=parent.id)
                                .order_by(DirectoryEntry.data_year.desc())
                                .first())

                if not latest_entry or not latest_entry.latitude or not latest_entry.longitude:
                    validation_results['critical_issues'].append({
                        'ncessch': ncessch,
                        'issue': 'No coordinates available'
                    })
                    continue

                existing_splits = School.query.filter(or_(
                    School.ncessch == f"{ncessch}-es",
                    School.ncessch == f"{ncessch}-ms",
                    School.ncessch == f"{ncessch}-hs"
                )).all()

                if existing_splits:
                    validation_results['warnings'].append({
                        'ncessch': ncessch,
                        'issue': 'School already has splits configured'
                    })

                splits_data = []
                splits = school.get('splits', [])

                for split in splits:
                    # Create the input expected by transform_split_data
                    split_input = {
                        'ncessch': split['ncessch'],
                        'school_name': split.get('displayName') or split.get('name'),
                        'split_type': split.get('splitType', ''),
                        'grades_served': split.get('gradesServed', []),
                        'team_name': school.get('team_name'),
                        'street': split.get('address', {}).get('street', ''),
                        'city': split.get('address', {}).get('city', ''),
                        'state': split.get('address', {}).get('state', ''),
                        'zip': split.get('address', {}).get('zip', '')
                    }
                    transformed = transform_split_data(split_input)
                    splits_data.append(transformed)
                    
                    # Add each split as a separate entry in ready_for_processing
                    validation_results['data']['ready_for_processing'].append({
                        'ncessch': split['ncessch'],
                        'parent_ncessch': ncessch,
                        'name': split.get('displayName') or split.get('name'),
                        'parent_name': parent.school_name,
                        'type': 'split',
                        'split_data': transformed,
                        'team_name': school.get('team_name'),
                        'coordinates': {
                            'latitude': latest_entry.latitude,
                            'longitude': latest_entry.longitude
                        }
                    })
                                    
                is_valid, error = validate_split_configuration(ncessch, splits_data)
                
                if not is_valid:
                    validation_results['critical_issues'].append({
                        'ncessch': ncessch,
                        'issue': error
                    })
                    continue

            else:
                # Standard school processing - keeping original structure
                ncessch = school['ncessch'] if isinstance(school, dict) else str(school).strip()

                
                parent = School.query.filter_by(ncessch=ncessch).first()
                if not parent:
                    validation_results['critical_issues'].append({
                        'ncessch': ncessch,
                        'issue': 'School not found'
                    })
                    continue

                latest_entry = (DirectoryEntry.query
                                .filter_by(school_id=parent.id)
                                .order_by(DirectoryEntry.data_year.desc())
                                .first())

                if not latest_entry or not latest_entry.latitude or not latest_entry.longitude:
                    validation_results['critical_issues'].append({
                        'ncessch': ncessch,
                        'issue': 'No coordinates available'
                    })
                    continue

                validation_results['data']['ready_for_processing'].append({
                    'ncessch': ncessch,
                    'name': parent.school_name,
                    'type': process_type,
                    'coordinates': {
                        'latitude': latest_entry.latitude,
                        'longitude': latest_entry.longitude
                    }
                })

       # Initialize Firestore for schools to be processed
        current_app.logger.info(f"Initializing firestore for {len(validation_results['data']['ready_for_processing'])} schools")
        db = firestore.client()
        batch = db.batch()

        for school_info in validation_results['data']['ready_for_processing']:
            try:
                # Extract team_name if present in the school data
                team_name = school_info.get('team_name')
                
                # For split schools, check split_data as well
                if school_info['type'] == 'split' and not team_name:
                    if 'split_data' in school_info and 'team_name' in school_info['split_data']:
                        team_name = school_info['split_data']['team_name']
                
                if team_name:
                    current_app.logger.info(f"Found team name '{team_name}' for school {school_info['ncessch']}")
                    
                if school_info['type'] == 'split':
                    # Split school Firestore setup - create status for each split
                    status_ref = (db.collection('schools')
                                .document(school_info['ncessch'])
                                .collection('processing_status')
                                .document('current'))
                    
                    status_data = {
                        'started_at': firestore.SERVER_TIMESTAMP,
                        'type': 'split',
                        'overall_status': 'processing',
                        'parent_ncessch': school_info['parent_ncessch'],
                        'parent_name': school_info['parent_name'],
                        'stages': {
                            'validation': {'status': 'completed'},
                            'geocoding': {'status': 'pending'},
                            'nces_data': {'status': 'pending'},
                            'esri_data': {'status': 'pending'},
                            'projections': {'status': 'pending'},
                            'metrics': {'status': 'pending'},
                            'team_assignment': {'status': 'pending' if team_name else 'not_applicable'}
                        }
                    }
                    
                    # Add team_name to status if present
                    if team_name:
                        status_data['team_name'] = team_name
                    
                    batch.set(status_ref, status_data)

                    # Create/update main document for split school
                    school_ref = db.collection('schools').document(school_info['ncessch'])
                    school_doc_data = {
                        'name': school_info['split_data']['school_name'],
                        'type': 'split',
                        'parent_ncessch': school_info['parent_ncessch'],
                        'parent_name': school_info['parent_name']
                    }
                    
                    # Add team_name to school document if present
                    if team_name:
                        school_doc_data['team_name'] = team_name
                        
                    batch.set(school_ref, school_doc_data, merge=True)
                else:
                    # Standard school Firestore setup
                    status_ref = (db.collection('schools')
                                .document(school_info['ncessch'])
                                .collection('processing_status')
                                .document('current'))
                    
                    status_data = {
                        'started_at': firestore.SERVER_TIMESTAMP,
                        'type': 'standard',
                        'overall_status': 'processing',
                        'stages': {
                            'validation': {'status': 'completed'},
                            'nces_data': {'status': 'not_applicable'},
                            'esri_data': {'status': 'pending'},
                            'projections': {'status': 'pending'},
                            'metrics': {'status': 'pending'},
                            'team_assignment': {'status': 'pending' if team_name else 'not_applicable'}
                        }
                    }
                    
                    # Add team_name to status if present
                    if team_name:
                        status_data['team_name'] = team_name
                    
                    batch.set(status_ref, status_data)
                    
                    # Update the school document itself
                    school_ref = db.collection('schools').document(school_info['ncessch'])
                    school_doc_data = {
                        'name': school_info['name'],
                        'type': 'standard'
                    }
                    
                    # Add team_name to school document if present
                    if team_name:
                        school_doc_data['team_name'] = team_name
                        
                    batch.set(school_ref, school_doc_data, merge=True)

            except Exception as e:
                current_app.logger.error(f"Error creating Firestore documents: {str(e)}")

        # Commit all Firestore updates
        batch.commit()

        # Process schools with task manager
        task_manager = TaskManager()
        results = {
            'processed': [],
            'failed': [],
            'skipped': []
        }

        for school_info in validation_results['data']['ready_for_processing']:
            try:
                current_app.logger.info(f"Processing school with info: {school_info}")
                if school_info['type'] == 'split':
                    current_app.logger.info(f"Processing split school: {school_info['ncessch']}")
                    result = task_manager.process_split_school(school_info)
                else:
                    result = task_manager.process_standard_school(school_info)

                results['processed'].append({
                    'ncessch': school_info['ncessch'],
                    'name': school_info['name'],
                    'type': school_info['type'],
                    'tasks': result['tasks']
                })

                current_app.logger.info(f"Processing tasks created for school {school_info['ncessch']}")    

            except Exception as e:
                results['failed'].append({
                    'ncessch': school_info['ncessch'],
                    'name': school_info['name'],
                    'error': str(e)
                })

        return jsonify({
            'status': 'processing_started',
            'job_ids': [result['tasks'] for result in results['processed']],
            'processing_details': {
                'processed': results['processed'],
                'failed': results['failed'],
                'skipped': results['skipped']
            }
        })

    except Exception as e:
        current_app.logger.error(f"Bulk processing error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Route to clean up processing data for a school
@unified_bp.route('/cleanup-processing/<string:ncessch>', methods=['POST'])
@firebase_auth_required()
@admin_required
async def cleanup_processing(ncessch):
    """Clean up all processing data for a school"""
    try:
        # Delete ESRI data
        if not delete_esri_data(ncessch):
            return jsonify({
                'status': 'error',
                'error': 'Failed to delete ESRI data'
            }), 500

        # Delete Firestore data
        db = firestore.client()
        school_ref = db.collection('schools').document(ncessch)
        
        batch = db.batch()
        
        # Delete projections
        proj_ref = school_ref.collection('public_projections').document('current')
        if proj_ref.get().exists:
            batch.delete(proj_ref)

        # Delete metrics
        metrics_ref = school_ref.collection('district_metrics').document('current')
        if metrics_ref.get().exists:
            batch.delete(metrics_ref)

        # Commit Firestore changes
        batch.commit()

        return jsonify({
            'status': 'success',
            'message': 'Processing data cleaned up successfully'
        })

    except Exception as e:
        current_app.logger.error(f"Cleanup error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500
    
# Route to reset processing status for a school
@unified_bp.route('/reset-processing/<string:ncessch>', methods=['POST'])
@firebase_auth_required()
@admin_required
def reset_processing(ncessch):
    """Reset processing status for a school"""
    try:
        current_app.logger.info(f"Resetting processing status for school {ncessch}")
        # Get Firestore instance
        db = firestore.client()
        
        # Important: Update the path to use the subcollection structure
        status_ref = (db.collection('schools')
                     .document(ncessch)
                     .collection('processing_status')
                     .document('current'))
        
        # Reset processing status - using overall_status instead of status
        status_ref.set({
            'overall_status': 'not_started',
            'started_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'stages': {
                'validation': {'status': 'pending'},
                'nces_data': {'status': 'pending'},
                'esri_data': {'status': 'pending'},
                'projections': {'status': 'pending'},
                'metrics': {'status': 'pending'}
            }
        })

        return jsonify({
            'status': 'success',
            'message': f'Processing status reset for school {ncessch}'
        })

    except Exception as e:
        current_app.logger.error(f"Reset processing error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@unified_bp.route('/assign-team', methods=['POST'])
def assign_team():
    """Handle team assignment for a school"""
    try:
        data = request.get_json()
        
        # Process team assignment
        result = process_team_assignment(data)
        
        if result.get('status') == 'error':
            return jsonify({
                'status': 'error',
                'error': result.get('error')
            }), 500
        
        return jsonify(result)
        
    except Exception as e:
        current_app.logger.error(f"Team assignment error: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500