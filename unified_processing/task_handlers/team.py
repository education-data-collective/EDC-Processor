from flask import current_app
from firebase_admin import firestore
from app.models import School, DirectoryEntry

def process_team_assignment(payload):
    """Process team assignment for a school"""
    try:
        ncessch = payload.get('ncessch')
        team_name = payload.get('team_name')
        user_id = payload.get('user_id', 'system')
        
        if not ncessch or not team_name:
            return {
                'status': 'error',
                'error': 'Missing required fields: ncessch and team_name'
            }
            
        current_app.logger.info(f"Processing team assignment for school {ncessch} to team {team_name}")
        
        # Get school data
        school = School.query.filter_by(ncessch=ncessch).first()
        if not school:
            error_msg = f"School not found: {ncessch}"
            current_app.logger.error(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }
            
        # Get directory entry for location info
        dir_entry = DirectoryEntry.query.filter_by(
            school_id=school.id
        ).order_by(DirectoryEntry.data_year.desc()).first()
        
        # Initialize Firestore
        db = firestore.client()
        
        # Look up team by name
        teams_ref = db.collection('teams')
        team_query = teams_ref.where('name', '==', team_name).limit(1).get()
        
        if len(team_query) > 0:
            # Team exists - get reference
            team_ref = teams_ref.document(team_query[0].id)
            current_app.logger.info(f"Found existing team: {team_name}")
        else:
            # Create new team
            team_ref = teams_ref.document()
            current_app.logger.info(f"Creating new team: {team_name}")
            
            team_data = {
                'name': team_name,
                'description': f"Automatically created during school processing",
                'schools': {},
                'metadata': {
                    'created': firestore.SERVER_TIMESTAMP,
                    'createdBy': user_id,
                    'lastUpdated': firestore.SERVER_TIMESTAMP,
                    'schoolCount': 0
                }
            }
            team_ref.set(team_data)
            
        # Add school to team
        school_data = {
            f'schools.{ncessch}': {
                'added': firestore.SERVER_TIMESTAMP,
                'addedBy': user_id,
                'schoolId': str(school.id),
                'ncessch': ncessch,
                'displayName': school.school_name,
                'city': dir_entry.city if dir_entry else None,
                'state': dir_entry.state if dir_entry else None
            },
            'metadata.lastUpdated': firestore.SERVER_TIMESTAMP,
            'metadata.schoolCount': firestore.Increment(1)
        }
        
        team_ref.update(school_data)
        
        # Also update the school status with team info
        status_ref = (db.collection('schools')
                     .document(ncessch)
                     .collection('processing_status')
                     .document('current'))
        
        # Only update if the status document exists
        status_doc = status_ref.get()
        if status_doc.exists:
            status_ref.update({
                'team_name': team_name,
                'team_id': team_ref.id,
                'stages.team_assignment': {
                    'status': 'completed',
                    'updated_at': firestore.SERVER_TIMESTAMP
                }
            })
            
        return {
            'status': 'success',
            'team_id': team_ref.id,
            'message': f"School {ncessch} assigned to team {team_name}"
        }
        
    except Exception as e:
        current_app.logger.error(f"Error assigning team: {str(e)}")
        return {
            'status': 'error',
            'error': str(e)
        }