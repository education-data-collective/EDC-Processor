from google.cloud import tasks_v2
from google.protobuf import duration_pb2, timestamp_pb2
import json
from flask import current_app
from datetime import datetime, timedelta
import os
from app.models import School, DirectoryEntry
from firebase_admin import firestore

class TaskManager:
    def __init__(self):
        self.project = os.environ.get('FIREBASE_PROJECT_ID')
        self.location = "us-central1"
        self.base_url = self.get_api_url()
        
        # Define different queues for different workloads
        self.queues = {
            'geocoding': 'school-geocoding-queue',  # Light, quick tasks
            'nces': 'school-nces-queue',           # Database operations
            'esri': 'school-esri-queue',           # Heavy processing
            'analysis': 'school-analysis-queue'     # Metrics and projections
        }
        
        # Configure queue settings
        self.queue_configs = {
            'geocoding': {
                'max_concurrent_dispatches': 5,
                'max_dispatches_per_second': 1,
                'retry_config': {
                    'max_attempts': 3,
                    'min_backoff': 30,  # 30 seconds
                    'max_backoff': 300  # 5 minutes
                }
            },
            'nces': {
                'max_concurrent_dispatches': 3,
                'max_dispatches_per_second': 0.5,
                'retry_config': {
                    'max_attempts': 3,
                    'min_backoff': 60,
                    'max_backoff': 600
                }
            },
            'esri': {
                'max_concurrent_dispatches': 2,
                'max_dispatches_per_second': 0.2,
                'retry_config': {
                    'max_attempts': 5,
                    'min_backoff': 300,
                    'max_backoff': 1800
                }
            },
            'analysis': {
                'max_concurrent_dispatches': 2,
                'max_dispatches_per_second': 0.2,
                'retry_config': {
                    'max_attempts': 3,
                    'min_backoff': 120,
                    'max_backoff': 900
                }
            }
        }

    def get_api_url(self):
        """Get API URL based on environment"""
        env = os.environ.get('FLASK_ENV', 'development')
        if env == 'development':
            return "http://localhost:5000"
        else:
            return f"https://{self.project}.uc.r.appspot.com"

    def create_task(self, queue_name, endpoint, payload, delay_seconds=0):
        """Create a Cloud Task with appropriate configuration"""
        if os.environ.get('FLASK_ENV') == 'development':
            # In development, process synchronously
            return self.process_task_synchronously(endpoint, payload)

        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.project, self.location, self.queues[queue_name])

        # Calculate schedule time if delay requested
        schedule_time = None
        if delay_seconds > 0:
            schedule_time = timestamp_pb2.Timestamp()
            schedule_time.FromDatetime(
                datetime.utcnow() + timedelta(seconds=delay_seconds)
            )

        # Configure task
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{self.base_url}/api/unified/{endpoint}",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode()
            },
            "dispatch_deadline": duration_pb2.Duration(
                seconds=self.queue_configs[queue_name].get('dispatch_deadline', 600)
            )
        }

        if schedule_time:
            task["schedule_time"] = schedule_time

        # Add OIDC authentication for non-development environments
        if os.environ.get('FLASK_ENV') != 'development':
            service_account_email = f"{self.project}@appspot.gserviceaccount.com"
            task["http_request"]["oidc_token"] = {
                "service_account_email": service_account_email,
                "audience": self.base_url
            }

        return client.create_task(request={"parent": parent, "task": task})

    def process_task_synchronously(self, endpoint, payload):
        """Process task synchronously in development"""
        try:
            if endpoint == "process-geocoding":
                from .task_handlers.geocoding import process_geocoding
                result = process_geocoding(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-nces":
                from .task_handlers.nces import process_nces_update
                result = process_nces_update(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-esri":
                from .task_handlers.esri import process_esri_data
                result = process_esri_data(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-projections":
                from .task_handlers.projections import process_projections
                result = process_projections(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-metrics":
                from .task_handlers.metrics import process_metrics
                import asyncio
                
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    # Run the async function
                    result = loop.run_until_complete(process_metrics(payload))
                    return {'name': 'local_task', 'result': result}
                finally:
                    # Clean up
                    loop.close()
                    asyncio.set_event_loop(None)
                
            else:
                raise ValueError(f"Unknown endpoint: {endpoint}")
                
        except Exception as e:
            current_app.logger.error(f"Synchronous processing error: {str(e)}")
            raise

    def process_split_school(self, school_info):
        """Process a split school component"""
        try:
            current_app.logger.info(f"Creating tasks for split school: {school_info['ncessch']}")
            current_app.logger.info(f"Initial school_info structure: {json.dumps(school_info, default=str)}")

            # Extract team_name if provided
            team_name = school_info.get('team_name')
            if team_name:
                current_app.logger.info(f"Found team name for split school: {team_name}")

            # Task 1: Geocoding for this split
            geocoding_payload = {
                'ncessch': school_info['ncessch'],
                'parent_ncessch': school_info['parent_ncessch'],
                'address': school_info['address']
            }
            current_app.logger.info(f"Geocoding payload: {json.dumps(geocoding_payload, default=str)}")
            
            geocoding_task = self.process_task_synchronously('process-geocoding', geocoding_payload)
            current_app.logger.info(f"Raw geocoding task result: {json.dumps(geocoding_task, default=str)}")

            # Check if geocoding returned valid coordinates and update split_data
            current_app.logger.info(f"Pre-coordinates update school_info: {json.dumps(school_info, default=str)}")
            
            try:
                if 'result' in geocoding_task:
                    current_app.logger.info("Found 'result' in geocoding_task")
                    # Handle nested result structure
                    if isinstance(geocoding_task['result'], dict) and 'result' in geocoding_task['result']:
                        coordinates = geocoding_task['result']['result']
                    else:
                        coordinates = geocoding_task['result']
                    current_app.logger.info(f"Extracted coordinates: {json.dumps(coordinates, default=str)}")
                else:
                    current_app.logger.info(f"Available keys in geocoding_task: {list(geocoding_task.keys())}")
                    coordinates = geocoding_task.get('coordinates', {})
                    current_app.logger.info(f"Extracted coordinates from 'coordinates' key: {json.dumps(coordinates, default=str)}")

                if 'split_data' not in school_info:
                    current_app.logger.info("Creating split_data dictionary")
                    school_info['split_data'] = {}
                
                if 'address' not in school_info['split_data']:
                    current_app.logger.info("Creating address dictionary in split_data")
                    school_info['split_data']['address'] = {}

                current_app.logger.info(f"Updating split_data address with coordinates: {json.dumps(coordinates, default=str)}")
                school_info['split_data']['address'].update(coordinates)
                
                current_app.logger.info(f"Post-coordinates update school_info: {json.dumps(school_info, default=str)}")
            except Exception as e:
                current_app.logger.error(f"Error updating school_info with coordinates: {str(e)}")
                current_app.logger.error(f"Current school_info structure: {json.dumps(school_info, default=str)}")
                raise

            # Task 2: NCES database updates (after geocoding)
            nces_task = self.create_task(
                'nces',
                'process-nces',
                {
                    'ncessch': school_info['ncessch'],
                    'parent_ncessch': school_info['parent_ncessch'],
                    'split_data': school_info['split_data']
                },
                delay_seconds=60  # Wait for geocoding to complete
            )

            # Task 3: ESRI processing - now happens after geocoding provides coordinates
            current_app.logger.info("Preparing ESRI task payload...")
            lat = school_info['split_data']['address'].get('latitude')
            lon = school_info['split_data']['address'].get('longitude')
            current_app.logger.info(f"Retrieved coordinates - latitude: {lat}, longitude: {lon}")
            
            esri_payload = {
                'ncessch': school_info['ncessch'],
                'coordinates': {
                    'latitude': lat,
                    'longitude': lon
                }
            }
            current_app.logger.info(f"ESRI payload: {json.dumps(esri_payload, default=str)}")

            # Task 3: ESRI processing
            esri_task = self.create_task(
                'esri',
                'process-esri',
                esri_payload,
                delay_seconds=180  # Wait for NCES updates
            )

            # Task 4: Projections 
            projections_task = self.create_task(
                'analysis',
                'process-projections',
                {
                    'ncessch': school_info['ncessch'],
                    'parent_ncessch': school_info['parent_ncessch']
                },
                delay_seconds=300  # Wait for ESRI processing
            )

            # Task 5: District Metrics
            metrics_task = self.create_task(
                'analysis',
                'process-metrics',
                {
                    'ncessch': school_info['ncessch'],
                    'parent_ncessch': school_info['parent_ncessch']
                },
                delay_seconds=420  # Wait for projections
            )

            # Task 6: Team Assignment (if team_name is provided)
            team_task_name = None
            if team_name:
                team_task = self.create_task(
                    'analysis',  # Using the analysis queue
                    'assign-team',
                    {
                        'ncessch': school_info['ncessch'],  # Use split school ncessch, not parent
                        'team_name': team_name,
                        'user_id': school_info.get('user_id', 'system')
                    },
                    delay_seconds=480  # Run after metrics
                )
                
                team_task_name = team_task['name'] if isinstance(team_task, dict) else team_task.name
                current_app.logger.info(f"Created team assignment task for split school {school_info['ncessch']} to team {team_name}")

            # Create tasks result with all tasks
            tasks_result = {
                'geocoding': geocoding_task['name'] if isinstance(geocoding_task, dict) else geocoding_task.name,
                'nces': nces_task['name'] if isinstance(nces_task, dict) else nces_task.name,
                'esri': esri_task['name'] if isinstance(esri_task, dict) else esri_task.name,
                'projections': projections_task['name'] if isinstance(projections_task, dict) else projections_task.name,
                'metrics': metrics_task['name'] if isinstance(metrics_task, dict) else metrics_task.name,
            }
            
            # Add team assignment task if created
            if team_task_name:
                tasks_result['team_assignment'] = team_task_name

            return {
                'status': 'tasks_created',
                'tasks': tasks_result
            }

        except Exception as e:
            current_app.logger.error(f"Task creation error for split {school_info['ncessch']}: {str(e)}")
            raise

    def process_standard_school(self, school_info):
        """Orchestrate standard school processing"""
        current_app.logger.info(f"Beginning processing tasks for standard school: {school_info['ncessch']}")
        try:

            # Extract team_name if provided
            team_name = school_info.get('team_name')

            # Task 1: ESRI processing (includes nearby schools)
            esri_task = self.create_task(
                'esri',
                'process-esri',
                {
                    'ncessch': school_info['ncessch'],
                    'coordinates': school_info['coordinates']
                }
            )

            # Task 2: Projections
            projections_task = self.create_task(
                'analysis',
                'process-projections',
                {'ncessch': school_info['ncessch']},
                delay_seconds=120  # Wait for ESRI processing
            )

            # Task 3: District Metrics
            metrics_task = self.create_task(
                'analysis',
                'process-metrics',
                {'ncessch': school_info['ncessch']},
                delay_seconds=240  # Wait for projections
            )

            # Task 4: Team Assignment
            team_task_name = None
            if team_name:
                team_task = self.create_task(
                    'analysis',  # Using the analysis queue
                    'assign-team',
                    {
                        'ncessch': school_info['ncessch'],
                        'team_name': team_name,
                        'user_id': school_info.get('user_id', 'system')
                    },
                    delay_seconds=300  # Run after metrics
                )
                
                team_task_name = team_task['name'] if isinstance(team_task, dict) else team_task.name
                current_app.logger.info(f"Created team assignment task for school {school_info['ncessch']} to team {team_name}")

            # Handle both development and production mode returns
            tasks_result = {
                'esri': esri_task['name'] if isinstance(esri_task, dict) else esri_task.name,
                'projections': projections_task['name'] if isinstance(projections_task, dict) else projections_task.name,
                'metrics': metrics_task['name'] if isinstance(metrics_task, dict) else metrics_task.name,
            }
            
            # Add team assignment task if created
            if team_task_name:
                tasks_result['team_assignment'] = team_task_name
            
            return {
                'status': 'tasks_created',
                'tasks': tasks_result
            }

        except Exception as e:
            current_app.logger.error(f"Task creation error: {str(e)}")
            raise

    def assign_school_to_team(self, ncessch, team_name, user_id=None):
        """
        Assign a school to a team - creates the team if it doesn't exist.
        This can be called directly from process_school_task or any other task handler.
        """
        try:
            # Get school data from the database
            school = School.query.filter_by(ncessch=ncessch).first()
            if not school:
                current_app.logger.error(f"Cannot assign school {ncessch} to team - school not found")
                return False
                
            # Get directory entry for location info
            dir_entry = DirectoryEntry.query.filter_by(
                school_id=school.id
            ).order_by(DirectoryEntry.data_year.desc()).first()
            
            # Initialize Firestore
            db = firestore.client()
            
            # Check if team exists by name
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
                    'description': f"Automatically created during school processing",
                    'schools': {},
                    'metadata': {
                        'created': firestore.SERVER_TIMESTAMP,
                        'createdBy': user_id or 'system',
                        'lastUpdated': firestore.SERVER_TIMESTAMP,
                        'schoolCount': 0
                    }
                }
                team_ref.set(team_data)
            
            # Add school to team
            school_data = {
                f'schools.{ncessch}': {
                    'added': firestore.SERVER_TIMESTAMP,
                    'addedBy': user_id or 'system',
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
            current_app.logger.info(f"Added school {ncessch} to team {team_name}")
            return True
            
        except Exception as e:
            current_app.logger.error(f"Error assigning school to team: {str(e)}")
            return False
        
    def process_task_synchronously(self, endpoint, payload):
        """Process task synchronously in development"""
        try:
            if endpoint == "process-geocoding":
                from .task_handlers.geocoding import process_geocoding
                result = process_geocoding(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-nces":
                from .task_handlers.nces import process_nces_update
                result = process_nces_update(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-esri":
                from .task_handlers.esri import process_esri_data
                result = process_esri_data(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-projections":
                from .task_handlers.projections import process_projections
                result = process_projections(payload)
                return {'name': 'local_task', 'result': result}
                
            elif endpoint == "process-metrics":
                from .task_handlers.metrics import process_metrics
                import asyncio
                
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    # Run the async function
                    result = loop.run_until_complete(process_metrics(payload))
                    return {'name': 'local_task', 'result': result}
                finally:
                    # Clean up
                    loop.close()
                    asyncio.set_event_loop(None)
            
            elif endpoint == "assign-team":
                from .task_handlers.team import process_team_assignment
                result = process_team_assignment(payload)
                return {'name': 'local_task', 'result': result}
                
            else:
                raise ValueError(f"Unknown endpoint: {endpoint}")
                
        except Exception as e:
            current_app.logger.error(f"Synchronous processing error: {str(e)}")
            raise