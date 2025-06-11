from flask import current_app
from app import db
from firebase_admin import firestore
from app.models import School, DirectoryEntry, EsriData
from app.utils.school_utils import validate_split_configuration, transform_split_data
from sqlalchemy import or_
from sqlalchemy.orm import scoped_session, sessionmaker

def get_db_session(bind_key=None):
    """Get a database session with optional bind"""
    try:
        if not bind_key:
            return db.session
        
        engine = db.get_engine(current_app, bind=bind_key)
        Session = scoped_session(sessionmaker(bind=engine))
        return Session()
    except Exception as e:
        current_app.logger.error(f"Error getting database session for bind '{bind_key}': {str(e)}")
        raise

def success(data=None, message=None):
    """Standard success response"""
    response = {'status': 'success'}
    if data is not None:
        response['data'] = data
    if message is not None:
        response['message'] = message
    return response

def error(message, code=400):
    """Standard error response"""
    return {
        'status': 'error',
        'error': str(message)
    }, code

# Validation helper functions
def validate_single_school(ncessch, options=None):
    """Validate a single school and get its processing requirements"""
    try:
        # Basic validation
        school = School.query.filter_by(ncessch=ncessch).first()
        if not school:
            return {
                'status': 'error',
                'error': 'School not found'
            }, 404

        # Check for existing splits
        existing_splits = School.query.filter(or_(
            School.ncessch == f"{ncessch}-es",
            School.ncessch == f"{ncessch}-ms",
            School.ncessch == f"{ncessch}-hs"
        )).all()

        # Get NCES data status
        dir_entry = DirectoryEntry.query.filter_by(
            school_id=school.id
        ).order_by(DirectoryEntry.data_year.desc()).first()

        # Check ESRI data status
        esri_data = EsriData.query.filter_by(
            ncessch=ncessch,
            has_data=1
        ).first()

        # Get Firestore data
        db = firestore.client()
        firestore_data = {
            'projections': False,
            'metrics': False
        }
        school_ref = db.collection('schools').document(ncessch)
        
        # Check projections
        proj_ref = school_ref.collection('public_projections').document('current')
        if proj_ref.get().exists:
            firestore_data['projections'] = True

        # Check district metrics
        metrics_ref = school_ref.collection('district_metrics').document('current')
        if metrics_ref.get().exists:
            firestore_data['metrics'] = True

        return {
            'status': 'success',
            'isValid': True,
            'school_data': {
                'id': school.id,
                'ncessch': school.ncessch,
                'name': school.school_name,
                'city': dir_entry.city if dir_entry else None,
                'state': dir_entry.state if dir_entry else None
            },
            'data_status': {
                'has_nces_data': bool(dir_entry),
                'has_esri_data': bool(esri_data),
                'has_projections': firestore_data['projections'],
                'has_metrics': firestore_data['metrics']
            },
            'existing_splits': [{
                'ncessch': split.ncessch,
                'name': split.school_name
            } for split in existing_splits] if existing_splits else [],
            'processing_type': 'split' if existing_splits else 'standard'
        }

    except Exception as e:
        current_app.logger.error(f"Validation error: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }, 500

def validate_schools_structure(schools_data):
    """Validate structure of multiple schools"""
    # Extract schools list from potential nested structure
    if isinstance(schools_data, dict):
        if 'schools' in schools_data:
            schools_data = schools_data['schools']
        
    validation_results = {
        'status': 'validation_complete',
        'critical_issues': [],
        'warnings': [],
        'needs_splitting': [],
        'data': {
            'ready_for_processing': [],
            'needs_team_assignment': [],
            'invalid_schools': [],
            'team_assignments': []
        }
    }

    current_app.logger.info(f"Starting validation of {len(schools_data)} schools")

    # Group schools by parent school
    parent_schools = {}
    for school in schools_data:
        is_split = 'parent_ncessch' in school
        raw_ncessch = str(school.get('parent_ncessch' if is_split else 'ncessch', '')).strip()
        
        if not raw_ncessch:
            validation_results['critical_issues'].append({
                'issue': 'Missing NCES ID',
                'school': school
            })
            continue

        base_ncessch = raw_ncessch.split('-')[0] if '-' in raw_ncessch else raw_ncessch
        if base_ncessch.isdigit():
            base_ncessch = base_ncessch.zfill(12)
        else:
            validation_results['critical_issues'].append({
                'issue': 'NCES ID must contain only digits',
                'school': school
            })
            continue

        if base_ncessch not in parent_schools:
            parent_schools[base_ncessch] = []
        parent_schools[base_ncessch].append(school)

    current_app.logger.info(f"Grouped into {len(parent_schools)} parent schools")

    # Validate each parent school group
    for parent_ncessch, schools in parent_schools.items():
        current_app.logger.info(f"Processing parent school {parent_ncessch} with {len(schools)} components")
        
        parent_validation = validate_single_school(parent_ncessch)
        # Handle the case where parent_validation is a tuple (error case with status code)
        if isinstance(parent_validation, tuple):
            parent_validation = parent_validation[0]  # Extract the dictionary from the tuple
        
        if parent_validation.get('status') == 'error':
            validation_results['critical_issues'].append({
                'ncessch': parent_ncessch,
                'issue': parent_validation.get('error', 'Parent school validation failed')
            })
            continue

        parent_data = parent_validation['school_data']
        is_split = 'parent_ncessch' in schools[0]
        
        if is_split:
            current_app.logger.info(f"Processing split configuration for {parent_ncessch}")
            
            splits_data = []
            
            # Transform each split component directly from schools array
            for school in schools:
                split_data = school['split_data']
                
                # Ensure required fields are present
                if not all(key in split_data for key in ['ncessch', 'school_name', 'split_type', 'grades_served']):
                    validation_results['critical_issues'].append({
                        'ncessch': parent_ncessch,
                        'issue': f'Missing required fields in split data for {split_data.get("ncessch")}'
                    })
                    continue
                
                # Validate address fields are present
                required_address_fields = ['street', 'city', 'state', 'zip']
                if not all(split_data.get(field) for field in required_address_fields):
                    validation_results['critical_issues'].append({
                        'ncessch': parent_ncessch,
                        'issue': f'Missing required address fields for {split_data.get("ncessch")}'
                    })
                    continue
                
                splits_data.append(split_data)
                current_app.logger.info(f"Added split config for {split_data['school_name']} ({split_data['ncessch']})")
            
            is_valid, error = validate_split_configuration(parent_ncessch, splits_data)
            
            if not is_valid:
                validation_results['critical_issues'].append({
                    'ncessch': parent_ncessch,
                    'issue': error
                })
                continue

            if parent_validation.get('existing_splits'):
                validation_results['warnings'].append({
                    'ncessch': parent_ncessch,
                    'issue': 'School already has splits configured'
                })

            # Add each split component to ready_for_processing
            for split in splits_data:
                validation_results['data']['ready_for_processing'].append({
                    'ncessch': split['ncessch'],
                    'parent_ncessch': parent_ncessch,
                    'name': split['school_name'],
                    'parent_name': parent_data['name'],
                    'type': 'split',
                    'split_data': split,
                    'team_name': split.get('team_name'),
                    'address': {  # Add address data for geocoding
                        'street': split['street'],
                        'city': split['city'],
                        'state': split['state'],
                        'zip': split['zip']
                    }
                })
                current_app.logger.info(f"Added split {split['ncessch']} to ready_for_processing")

        else:
            current_app.logger.info(f"Processing standard school {parent_ncessch}")
            school_data = schools[0]
            
            if parent_validation.get('existing_splits'):
                validation_results['needs_splitting'].append({
                    **parent_data,
                    'splits': parent_validation['existing_splits']
                })
                continue

            # Get coordinates for standard school from DirectoryEntry
            parent = School.query.filter_by(ncessch=parent_ncessch).first()
            latest_entry = (DirectoryEntry.query
                            .filter_by(school_id=parent.id)
                            .order_by(DirectoryEntry.data_year.desc())
                            .first())

            if not latest_entry or not latest_entry.latitude or not latest_entry.longitude:
                validation_results['critical_issues'].append({
                    'ncessch': parent_ncessch,
                    'issue': 'No coordinates available'
                })
                continue

            if not school_data.get('team_name'):
                validation_results['data']['needs_team_assignment'].append(parent_data)
            else:
                validation_results['data']['ready_for_processing'].append({
                    'ncessch': parent_ncessch,
                    'name': parent_data['name'],
                    'type': 'standard',
                    'team_name': school_data.get('team_name'),
                    'coordinates': {
                        'latitude': latest_entry.latitude,
                        'longitude': latest_entry.longitude
                    }
                })

    validation_results['can_proceed'] = (
        len(validation_results['critical_issues']) == 0 and
        len(validation_results['data']['needs_team_assignment']) == 0
    )

    current_app.logger.info(f"Validation complete. Ready for processing: {len(validation_results['data']['ready_for_processing'])} schools")
    return validation_results

def update_overall_status(status_ref, stages):
    """Helper function to update overall status based on stage statuses"""
    try:
        doc = status_ref.get()
        if not doc.exists:
            return
            
        current_data = doc.to_dict()
        current_stages = current_data.get('stages', {})
        process_type = current_data.get('type', 'standard')
        
        # Define required stages based on process type
        required_stages = ['validation', 'esri_data', 'projections', 'metrics']
        if process_type == 'split':
            required_stages.append('nces_data')
        
        # Check if all required stages are completed
        all_completed = all(
            current_stages.get(stage, {}).get('status') in ['completed', 'not_applicable']
            for stage in required_stages
        )
        
        # Check if any required stage failed
        any_failed = any(
            current_stages.get(stage, {}).get('status') == 'failed'
            for stage in required_stages
        )
        
        # Update overall status
        new_status = 'completed' if all_completed else 'failed' if any_failed else 'processing'
        
        current_app.logger.info(f"Updating overall status to {new_status}. All completed: {all_completed}, Any failed: {any_failed}")
        current_app.logger.info(f"Stage statuses: {current_stages}")
        
        status_ref.update({
            'overall_status': new_status,
            'completed_at': firestore.SERVER_TIMESTAMP if new_status == 'completed' else None
        })
        
    except Exception as e:
        current_app.logger.error(f"Error updating overall status: {str(e)}")
        raise