"""
Team Assignment Processing

Handles team assignment for schools.
Not applicable to standalone location points.
"""

from flask import current_app
from firebase_admin import firestore
from models import School, SchoolLocation
from ..utils import success_response, error_response


async def process_team_assignment(payload):
    """Process team assignment for a school"""
    try:
        school_id = payload.get('school_id')
        team_name = payload.get('team_name')
        user_id = payload.get('user_id', 'system')
        
        current_app.logger.info(f"Processing team assignment for school {school_id} to team {team_name}")
        
        if not school_id or not team_name:
            raise ValueError("Missing required fields: school_id and team_name")
        
        school = School.query.get(school_id)
        if not school:
            raise ValueError(f"School not found: {school_id}")
        
        # Get current location for team assignment
        current_location = None
        for location in school.locations:
            if location.is_current:
                current_location = location
                break
        
        # Initialize Firestore
        db = firestore.client()
        
        # Look up team by name
        teams_ref = db.collection('teams')
        team_query = teams_ref.where('name', '==', team_name).limit(1).get()
        
        # Get or create team
        if len(team_query) > 0:
            # Team exists - get its reference
            team_ref = teams_ref.document(team_query[0].id)
            current_app.logger.info(f"Found existing team: {team_name} ({team_query[0].id})")
        else:
            # Create new team
            team_ref = teams_ref.document()
            current_app.logger.info(f"Creating new team: {team_name} ({team_ref.id})")
            
            team_data = {
                'name': team_name,
                'description': f"Automatically created during entity processing",
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
            f'schools.{school_id}': {
                'added': firestore.SERVER_TIMESTAMP,
                'addedBy': user_id,
                'schoolId': str(school.id),
                'displayName': school.name or f"School {school_id}",
                'city': current_location.location_point.city if current_location else None,
                'state': current_location.location_point.state if current_location else None
            },
            'metadata.lastUpdated': firestore.SERVER_TIMESTAMP,
            'metadata.schoolCount': firestore.Increment(1)
        }
        
        team_ref.update(school_data)
        
        current_app.logger.info(f"Successfully assigned school {school_id} to team {team_name}")
        
        return success_response({
            'school_id': school_id,
            'team_name': team_name,
            'team_id': team_ref.id,
            'action': 'assigned'
        }, f"School assigned to team {team_name}")
        
    except Exception as e:
        current_app.logger.error(f"Team assignment error: {str(e)}")
        return error_response(str(e))


def get_school_teams(school_id):
    """Get teams that a school is assigned to"""
    try:
        db = firestore.client()
        teams_ref = db.collection('teams')
        
        # Query for teams that contain this school
        teams_query = teams_ref.where(f'schools.{school_id}', '>', '').get()
        
        teams = []
        for team_doc in teams_query:
            team_data = team_doc.to_dict()
            school_assignment = team_data.get('schools', {}).get(str(school_id), {})
            
            teams.append({
                'team_id': team_doc.id,
                'team_name': team_data.get('name'),
                'assigned_at': school_assignment.get('added'),
                'assigned_by': school_assignment.get('addedBy')
            })
        
        return teams
        
    except Exception as e:
        current_app.logger.error(f"Error getting school teams: {str(e)}")
        return []


def remove_school_from_team(school_id, team_name=None, team_id=None):
    """Remove a school from a team"""
    try:
        db = firestore.client()
        teams_ref = db.collection('teams')
        
        # Find team by name or ID
        if team_id:
            team_ref = teams_ref.document(team_id)
        elif team_name:
            team_query = teams_ref.where('name', '==', team_name).limit(1).get()
            if not team_query:
                raise ValueError(f"Team not found: {team_name}")
            team_ref = teams_ref.document(team_query[0].id)
        else:
            raise ValueError("Either team_name or team_id must be provided")
        
        # Remove school from team
        team_ref.update({
            f'schools.{school_id}': firestore.DELETE_FIELD,
            'metadata.lastUpdated': firestore.SERVER_TIMESTAMP,
            'metadata.schoolCount': firestore.Increment(-1)
        })
        
        current_app.logger.info(f"Removed school {school_id} from team")
        return True
        
    except Exception as e:
        current_app.logger.error(f"Error removing school from team: {str(e)}")
        return False


def get_team_schools(team_name=None, team_id=None):
    """Get all schools assigned to a team"""
    try:
        db = firestore.client()
        teams_ref = db.collection('teams')
        
        # Find team by name or ID
        if team_id:
            team_doc = teams_ref.document(team_id).get()
        elif team_name:
            team_query = teams_ref.where('name', '==', team_name).limit(1).get()
            if not team_query:
                return []
            team_doc = team_query[0]
        else:
            return []
        
        if not team_doc.exists:
            return []
        
        team_data = team_doc.to_dict()
        schools = team_data.get('schools', {})
        
        team_schools = []
        for school_id, school_data in schools.items():
            team_schools.append({
                'school_id': school_id,
                'display_name': school_data.get('displayName'),
                'city': school_data.get('city'),
                'state': school_data.get('state'),
                'assigned_at': school_data.get('added'),
                'assigned_by': school_data.get('addedBy')
            })
        
        return team_schools
        
    except Exception as e:
        current_app.logger.error(f"Error getting team schools: {str(e)}")
        return [] 