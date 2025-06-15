import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

class FirebaseAnalysis:
    """
    Analyze Firebase data and create CSV reports for team_groups and teams relationships.
    """
    
    def __init__(self, service_account_path: str = None):
        """Initialize Firebase connection."""
        self.db = None
        self._initialize_firebase(service_account_path)
    
    def _initialize_firebase(self, service_account_path: str = None):
        """Initialize Firebase Admin SDK with service account credentials."""
        try:
            # Check if Firebase is already initialized
            if firebase_admin._apps:
                print("Firebase already initialized, using existing app.")
                self.db = firestore.client()
                return
            
            # Determine service account path
            if service_account_path is None:
                service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if service_account_path is None:
                    default_path = 'firebase-service-account.json'
                    if os.path.exists(default_path):
                        service_account_path = default_path
                    else:
                        raise FileNotFoundError("Service account file not found.")
            
            if not os.path.exists(service_account_path):
                raise FileNotFoundError(f"Service account file not found: {service_account_path}")
            
            # Initialize Firebase
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            
            print(f"✅ Successfully connected to Firestore")
            
        except Exception as e:
            print(f"❌ Failed to initialize Firebase: {str(e)}")
            sys.exit(1)
    
    def explore_collection_structure(self, collection_name: str, sample_size: int = 5):
        """
        Explore the structure of a collection to understand its schema.
        
        Args:
            collection_name: Name of the collection to explore
            sample_size: Number of documents to sample for structure analysis
        """
        print(f"\n🔍 Exploring '{collection_name}' collection structure:")
        print("=" * 50)
        
        try:
            collection_ref = self.db.collection(collection_name)
            docs = collection_ref.limit(sample_size).stream()
            
            documents = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id
                documents.append(doc_data)
            
            if not documents:
                print(f"❌ No documents found in '{collection_name}'")
                return
            
            print(f"📊 Found {len(documents)} sample documents")
            print("\n📋 Field analysis:")
            
            # Analyze field structure
            all_fields = set()
            field_types = {}
            field_examples = {}
            
            for doc in documents:
                for field, value in doc.items():
                    all_fields.add(field)
                    field_type = type(value).__name__
                    
                    if field not in field_types:
                        field_types[field] = set()
                        field_examples[field] = []
                    
                    field_types[field].add(field_type)
                    if len(field_examples[field]) < 3:  # Keep up to 3 examples
                        field_examples[field].append(str(value)[:100])  # Truncate long values
            
            for field in sorted(all_fields):
                types_str = ', '.join(sorted(field_types[field]))
                examples_str = ' | '.join(field_examples[field][:2])
                print(f"  • {field:<20} ({types_str}): {examples_str}")
            
            print(f"\n📄 Sample document:")
            if documents:
                sample_doc = documents[0]
                for key, value in sample_doc.items():
                    print(f"  {key}: {value}")
                    
            return documents
            
        except Exception as e:
            print(f"❌ Error exploring collection '{collection_name}': {str(e)}")
            return []
    
    def get_all_documents(self, collection_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all documents from a collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            List of all documents in the collection
        """
        try:
            print(f"📥 Retrieving all documents from '{collection_name}'...")
            collection_ref = self.db.collection(collection_name)
            docs = collection_ref.stream()
            
            documents = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id
                documents.append(doc_data)
            
            print(f"✅ Retrieved {len(documents)} documents from '{collection_name}'")
            return documents
            
        except Exception as e:
            print(f"❌ Error retrieving documents from '{collection_name}': {str(e)}")
            return []
    
    def analyze_team_relationships(self):
        """
        Analyze the relationship between team_groups and teams collections.
        """
        print("\n🔗 Analyzing team_groups and teams relationships...")
        print("=" * 60)
        
        # Get all team_groups and teams
        team_groups = self.get_all_documents('team_groups')
        teams = self.get_all_documents('teams')
        
        if not team_groups or not teams:
            print("❌ Could not retrieve data from both collections")
            return None, None
        
        print(f"📊 Found {len(team_groups)} team groups and {len(teams)} teams")
        
        return team_groups, teams
    
    def create_team_analysis_csv(self, output_filename: str = None):
        """
        Create a CSV file with team_groups, teams, and their relationships.
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"team_analysis_{timestamp}.csv"
        
        print(f"\n📊 Creating team analysis CSV: {output_filename}")
        print("=" * 50)
        
        # First, explore the structure of both collections
        print("🔍 Exploring team_groups structure:")
        team_groups_sample = self.explore_collection_structure('team_groups', sample_size=3)
        
        print("\n🔍 Exploring teams structure:")
        teams_sample = self.explore_collection_structure('teams', sample_size=3)
        
        # Get all data
        team_groups, teams = self.analyze_team_relationships()
        
        if not team_groups or not teams:
            return
        
        # Create analysis data
        analysis_data = []
        
        # Process team_groups and their embedded teams
        for group in team_groups:
            group_id = group.get('_id', '')
            group_name = group.get('name', 'Unknown')
            group_description = group.get('description', '')
            group_metadata = group.get('metadata', {})
            embedded_teams = group.get('teams', {})
            
            if embedded_teams and isinstance(embedded_teams, dict):
                # Process each embedded team
                for team_id, team_info in embedded_teams.items():
                    # Find the corresponding full team document
                    full_team = next((t for t in teams if t.get('_id') == team_id), None)
                    
                    analysis_data.append({
                        'team_group_id': group_id,
                        'team_group_name': group_name,
                        'team_group_description': group_description,
                        'group_team_count': group_metadata.get('teamCount', 0),
                        'group_created': group_metadata.get('created', ''),
                        'group_last_updated': group_metadata.get('lastUpdated', ''),
                        'team_id': team_id,
                        'team_name_in_group': team_info.get('name', 'Unknown'),
                        'team_added_to_group': team_info.get('added', ''),
                        'team_added_by': team_info.get('addedBy', ''),
                        'connection_type': 'embedded_in_group',
                        # Full team document data (if found)
                        'team_full_name': full_team.get('name', '') if full_team else 'NOT_FOUND',
                        'team_school_count': full_team.get('metadata', {}).get('schoolCount', 0) if full_team else 0,
                        'team_created': full_team.get('metadata', {}).get('created', '') if full_team else '',
                        'team_last_updated': full_team.get('metadata', {}).get('lastUpdated', '') if full_team else '',
                        'team_found_in_collection': 'YES' if full_team else 'NO'
                    })
            else:
                # Group with no embedded teams
                analysis_data.append({
                    'team_group_id': group_id,
                    'team_group_name': group_name,
                    'team_group_description': group_description,
                    'group_team_count': group_metadata.get('teamCount', 0),
                    'group_created': group_metadata.get('created', ''),
                    'group_last_updated': group_metadata.get('lastUpdated', ''),
                    'team_id': '',
                    'team_name_in_group': 'NO_TEAMS_FOUND',
                    'team_added_to_group': '',
                    'team_added_by': '',
                    'connection_type': 'empty_group',
                    'team_full_name': '',
                    'team_school_count': 0,
                    'team_created': '',
                    'team_last_updated': '',
                    'team_found_in_collection': ''
                })
        
        # Find teams that are NOT referenced in any team group
        all_referenced_team_ids = set()
        for group in team_groups:
            embedded_teams = group.get('teams', {})
            if isinstance(embedded_teams, dict):
                all_referenced_team_ids.update(embedded_teams.keys())
        
        unreferenced_teams = [t for t in teams if t.get('_id') not in all_referenced_team_ids]
        
        for team in unreferenced_teams:
            team_metadata = team.get('metadata', {})
            analysis_data.append({
                'team_group_id': 'NONE',
                'team_group_name': 'UNREFERENCED_TEAM',
                'team_group_description': '',
                'group_team_count': 0,
                'group_created': '',
                'group_last_updated': '',
                'team_id': team.get('_id', ''),
                'team_name_in_group': '',
                'team_added_to_group': '',
                'team_added_by': '',
                'connection_type': 'unreferenced_team',
                'team_full_name': team.get('name', 'Unknown'),
                'team_school_count': team_metadata.get('schoolCount', 0),
                'team_created': team_metadata.get('created', ''),
                'team_last_updated': team_metadata.get('lastUpdated', ''),
                'team_found_in_collection': 'YES'
            })
        
        # Create DataFrame and save to CSV
        if analysis_data:
            df = pd.DataFrame(analysis_data)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            embedded_count = len([r for r in analysis_data if r['connection_type'] == 'embedded_in_group'])
            unreferenced_count = len([r for r in analysis_data if r['connection_type'] == 'unreferenced_team'])
            empty_group_count = len([r for r in analysis_data if r['connection_type'] == 'empty_group'])
            
            print(f"✅ CSV created successfully: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            print(f"📈 Summary:")
            print(f"  • Team groups: {len(team_groups)}")
            print(f"  • Teams: {len(teams)}")
            print(f"  • Teams embedded in groups: {embedded_count}")
            print(f"  • Unreferenced teams: {unreferenced_count}")
            print(f"  • Empty groups: {empty_group_count}")
            print(f"  • Total team references in groups: {len(all_referenced_team_ids)}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None

    def create_detailed_team_school_analysis_csv(self, output_filename: str = None):
        """
        Create a detailed CSV file with team_groups, teams, schools, and their relationships.
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"detailed_team_school_analysis_{timestamp}.csv"
        
        print(f"\n📊 Creating detailed team-school analysis CSV: {output_filename}")
        print("=" * 60)
        
        # Get all data
        team_groups, teams = self.analyze_team_relationships()
        
        if not team_groups or not teams:
            return
        
        # Create detailed analysis data
        analysis_data = []
        
        # Process team_groups and their embedded teams with school details
        for group in team_groups:
            group_id = group.get('_id', '')
            group_name = group.get('name', 'Unknown')
            group_description = group.get('description', '')
            group_metadata = group.get('metadata', {})
            embedded_teams = group.get('teams', {})
            
            if embedded_teams and isinstance(embedded_teams, dict):
                # Process each embedded team
                for team_id, team_info in embedded_teams.items():
                    # Find the corresponding full team document
                    full_team = next((t for t in teams if t.get('_id') == team_id), None)
                    
                    if full_team:
                        team_schools = full_team.get('schools', {})
                        team_metadata = full_team.get('metadata', {})
                        
                        if team_schools and isinstance(team_schools, dict):
                            # Create a row for each school in the team
                            for school_id, school_info in team_schools.items():
                                analysis_data.append({
                                    'team_group_id': group_id,
                                    'team_group_name': group_name,
                                    'team_group_description': group_description,
                                    'group_team_count': group_metadata.get('teamCount', 0),
                                    'group_created': group_metadata.get('created', ''),
                                    'group_last_updated': group_metadata.get('lastUpdated', ''),
                                    'team_id': team_id,
                                    'team_name_in_group': team_info.get('name', 'Unknown'),
                                    'team_full_name': full_team.get('name', ''),
                                    'team_added_to_group': team_info.get('added', ''),
                                    'team_added_by': team_info.get('addedBy', ''),
                                    'team_school_count': team_metadata.get('schoolCount', 0),
                                    'team_created': team_metadata.get('created', ''),
                                    'team_last_updated': team_metadata.get('lastUpdated', ''),
                                    'connection_type': 'embedded_in_group',
                                    'school_id': school_id,
                                    'school_ncessch': school_info.get('ncessch', ''),
                                    'school_display_name': school_info.get('displayName', 'Unknown'),
                                    'school_city': school_info.get('city', ''),
                                    'school_state': school_info.get('state', ''),
                                    'school_added_to_team': school_info.get('added', ''),
                                    'school_added_by': school_info.get('addedBy', ''),
                                    'school_found_in_team': 'YES'
                                })
                        else:
                            # Team with no schools
                            analysis_data.append({
                                'team_group_id': group_id,
                                'team_group_name': group_name,
                                'team_group_description': group_description,
                                'group_team_count': group_metadata.get('teamCount', 0),
                                'group_created': group_metadata.get('created', ''),
                                'group_last_updated': group_metadata.get('lastUpdated', ''),
                                'team_id': team_id,
                                'team_name_in_group': team_info.get('name', 'Unknown'),
                                'team_full_name': full_team.get('name', ''),
                                'team_added_to_group': team_info.get('added', ''),
                                'team_added_by': team_info.get('addedBy', ''),
                                'team_school_count': team_metadata.get('schoolCount', 0),
                                'team_created': team_metadata.get('created', ''),
                                'team_last_updated': team_metadata.get('lastUpdated', ''),
                                'connection_type': 'embedded_in_group',
                                'school_id': '',
                                'school_ncessch': '',
                                'school_display_name': 'NO_SCHOOLS_FOUND',
                                'school_city': '',
                                'school_state': '',
                                'school_added_to_team': '',
                                'school_added_by': '',
                                'school_found_in_team': 'NO'
                            })
                    else:
                        # Team referenced in group but not found in teams collection
                        analysis_data.append({
                            'team_group_id': group_id,
                            'team_group_name': group_name,
                            'team_group_description': group_description,
                            'group_team_count': group_metadata.get('teamCount', 0),
                            'group_created': group_metadata.get('created', ''),
                            'group_last_updated': group_metadata.get('lastUpdated', ''),
                            'team_id': team_id,
                            'team_name_in_group': team_info.get('name', 'Unknown'),
                            'team_full_name': 'TEAM_NOT_FOUND',
                            'team_added_to_group': team_info.get('added', ''),
                            'team_added_by': team_info.get('addedBy', ''),
                            'team_school_count': 0,
                            'team_created': '',
                            'team_last_updated': '',
                            'connection_type': 'team_not_found',
                            'school_id': '',
                            'school_ncessch': '',
                            'school_display_name': 'TEAM_NOT_FOUND',
                            'school_city': '',
                            'school_state': '',
                            'school_added_to_team': '',
                            'school_added_by': '',
                            'school_found_in_team': 'NO'
                        })
        
        # Find teams that are NOT referenced in any team group
        all_referenced_team_ids = set()
        for group in team_groups:
            embedded_teams = group.get('teams', {})
            if isinstance(embedded_teams, dict):
                all_referenced_team_ids.update(embedded_teams.keys())
        
        unreferenced_teams = [t for t in teams if t.get('_id') not in all_referenced_team_ids]
        
        for team in unreferenced_teams:
            team_metadata = team.get('metadata', {})
            team_schools = team.get('schools', {})
            
            if team_schools and isinstance(team_schools, dict):
                # Create a row for each school in unreferenced teams
                for school_id, school_info in team_schools.items():
                    analysis_data.append({
                        'team_group_id': 'NONE',
                        'team_group_name': 'UNREFERENCED_TEAM',
                        'team_group_description': '',
                        'group_team_count': 0,
                        'group_created': '',
                        'group_last_updated': '',
                        'team_id': team.get('_id', ''),
                        'team_name_in_group': '',
                        'team_full_name': team.get('name', 'Unknown'),
                        'team_added_to_group': '',
                        'team_added_by': '',
                        'team_school_count': team_metadata.get('schoolCount', 0),
                        'team_created': team_metadata.get('created', ''),
                        'team_last_updated': team_metadata.get('lastUpdated', ''),
                        'connection_type': 'unreferenced_team',
                        'school_id': school_id,
                        'school_ncessch': school_info.get('ncessch', ''),
                        'school_display_name': school_info.get('displayName', 'Unknown'),
                        'school_city': school_info.get('city', ''),
                        'school_state': school_info.get('state', ''),
                        'school_added_to_team': school_info.get('added', ''),
                        'school_added_by': school_info.get('addedBy', ''),
                        'school_found_in_team': 'YES'
                    })
            else:
                # Unreferenced team with no schools
                analysis_data.append({
                    'team_group_id': 'NONE',
                    'team_group_name': 'UNREFERENCED_TEAM',
                    'team_group_description': '',
                    'group_team_count': 0,
                    'group_created': '',
                    'group_last_updated': '',
                    'team_id': team.get('_id', ''),
                    'team_name_in_group': '',
                    'team_full_name': team.get('name', 'Unknown'),
                    'team_added_to_group': '',
                    'team_added_by': '',
                    'team_school_count': team_metadata.get('schoolCount', 0),
                    'team_created': team_metadata.get('created', ''),
                    'team_last_updated': team_metadata.get('lastUpdated', ''),
                    'connection_type': 'unreferenced_team',
                    'school_id': '',
                    'school_ncessch': '',
                    'school_display_name': 'NO_SCHOOLS_IN_TEAM',
                    'school_city': '',
                    'school_state': '',
                    'school_added_to_team': '',
                    'school_added_by': '',
                    'school_found_in_team': 'NO'
                })
        
        # Create DataFrame and save to CSV
        if analysis_data:
            df = pd.DataFrame(analysis_data)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            # Calculate statistics
            total_schools = len([r for r in analysis_data if r['school_found_in_team'] == 'YES'])
            teams_with_schools = len(set([r['team_id'] for r in analysis_data if r['school_found_in_team'] == 'YES']))
            teams_without_schools = len([r for r in analysis_data if r['school_display_name'] in ['NO_SCHOOLS_FOUND', 'NO_SCHOOLS_IN_TEAM']])
            states_represented = len(set([r['school_state'] for r in analysis_data if r['school_state']]))
            
            print(f"✅ Detailed CSV created successfully: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            print(f"📈 Detailed Summary:")
            print(f"  • Team groups: {len(team_groups)}")
            print(f"  • Total teams: {len(teams)}")
            print(f"  • Total schools: {total_schools}")
            print(f"  • Teams with schools: {teams_with_schools}")
            print(f"  • Teams without schools: {teams_without_schools}")
            print(f"  • States represented: {states_represented}")
            print(f"  • Unreferenced teams: {len(unreferenced_teams)}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None

    def create_school_summary_csv(self, output_filename: str = None):
        """
        Create a summary CSV focused on school statistics by team and team group.
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"school_summary_{timestamp}.csv"
        
        print(f"\n📊 Creating school summary CSV: {output_filename}")
        print("=" * 50)
        
        # Get all data
        team_groups, teams = self.analyze_team_relationships()
        
        if not team_groups or not teams:
            return
        
        summary_data = []
        
        # Process each team group
        for group in team_groups:
            group_id = group.get('_id', '')
            group_name = group.get('name', 'Unknown')
            group_description = group.get('description', '')
            embedded_teams = group.get('teams', {})
            
            total_schools_in_group = 0
            teams_in_group = len(embedded_teams) if embedded_teams else 0
            teams_with_schools = 0
            states_in_group = set()
            
            if embedded_teams and isinstance(embedded_teams, dict):
                for team_id, team_info in embedded_teams.items():
                    full_team = next((t for t in teams if t.get('_id') == team_id), None)
                    
                    if full_team:
                        team_schools = full_team.get('schools', {})
                        if team_schools and isinstance(team_schools, dict):
                            teams_with_schools += 1
                            total_schools_in_group += len(team_schools)
                            for school_info in team_schools.values():
                                state = school_info.get('state', '')
                                if state:
                                    states_in_group.add(state)
            
            summary_data.append({
                'team_group_id': group_id,
                'team_group_name': group_name,
                'team_group_description': group_description,
                'teams_in_group': teams_in_group,
                'teams_with_schools': teams_with_schools,
                'teams_without_schools': teams_in_group - teams_with_schools,
                'total_schools_in_group': total_schools_in_group,
                'states_represented': len(states_in_group),
                'state_list': ', '.join(sorted(states_in_group)) if states_in_group else '',
                'avg_schools_per_team': round(total_schools_in_group / teams_in_group, 2) if teams_in_group > 0 else 0
            })
        
        # Add summary for unreferenced teams
        all_referenced_team_ids = set()
        for group in team_groups:
            embedded_teams = group.get('teams', {})
            if isinstance(embedded_teams, dict):
                all_referenced_team_ids.update(embedded_teams.keys())
        
        unreferenced_teams = [t for t in teams if t.get('_id') not in all_referenced_team_ids]
        
        if unreferenced_teams:
            unreferenced_schools = 0
            unreferenced_with_schools = 0
            unreferenced_states = set()
            
            for team in unreferenced_teams:
                team_schools = team.get('schools', {})
                if team_schools and isinstance(team_schools, dict):
                    unreferenced_with_schools += 1
                    unreferenced_schools += len(team_schools)
                    for school_info in team_schools.values():
                        state = school_info.get('state', '')
                        if state:
                            unreferenced_states.add(state)
            
            summary_data.append({
                'team_group_id': 'NONE',
                'team_group_name': 'UNREFERENCED_TEAMS',
                'team_group_description': 'Teams not assigned to any team group',
                'teams_in_group': len(unreferenced_teams),
                'teams_with_schools': unreferenced_with_schools,
                'teams_without_schools': len(unreferenced_teams) - unreferenced_with_schools,
                'total_schools_in_group': unreferenced_schools,
                'states_represented': len(unreferenced_states),
                'state_list': ', '.join(sorted(unreferenced_states)) if unreferenced_states else '',
                'avg_schools_per_team': round(unreferenced_schools / len(unreferenced_teams), 2) if unreferenced_teams else 0
            })
        
        # Create DataFrame and save to CSV
        if summary_data:
            df = pd.DataFrame(summary_data)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            print(f"✅ Summary CSV created successfully: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None

    def explore_users_structure(self, sample_size: int = 3):
        """
        Explore the structure of the users collection to understand funder relationships.
        
        Args:
            sample_size: Number of sample documents to analyze
        """
        print(f"\n🔍 Exploring 'users' collection structure:")
        print("=" * 50)
        
        try:
            # Get sample documents
            users_ref = self.db.collection('users')
            docs = list(users_ref.limit(sample_size).stream())
            
            if not docs:
                print("❌ No documents found in users collection")
                return []
            
            print(f"📊 Found {len(docs)} sample documents")
            
            # Analyze structure
            all_fields = {}
            
            for doc in docs:
                data = doc.to_dict()
                data['_id'] = doc.id
                
                for field, value in data.items():
                    if field not in all_fields:
                        all_fields[field] = []
                    
                    # Store sample values (first 2 per field)
                    if len(all_fields[field]) < 2:
                        if isinstance(value, dict):
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}...")
                        elif isinstance(value, list):
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}...")
                        else:
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}")
            
            # Print field analysis
            print(f"\n📋 Field analysis:")
            for field, samples in sorted(all_fields.items()):
                sample_str = " | ".join(samples)
                print(f"  • {field:<20} ({samples[0].split(':')[0]}): {sample_str}")
            
            # Print detailed sample document
            if docs:
                sample_doc = docs[0].to_dict()
                sample_doc['_id'] = docs[0].id
                print(f"\n📄 Sample document:")
                for key, value in sample_doc.items():
                    if isinstance(value, dict):
                        print(f"  {key}: {str(value)[:200]}...")
                    elif isinstance(value, list):
                        print(f"  {key}: {str(value)[:200]}...")
                    else:
                        print(f"  {key}: {value}")
            
            return [doc.to_dict() | {'_id': doc.id} for doc in docs]
            
        except Exception as e:
            print(f"❌ Error exploring users collection: {e}")
            return []

    def analyze_user_school_connections(self):
        """
        Analyze how users connect to schools and identify funder relationships.
        """
        print(f"\n🔗 Analyzing user-school connections and funder relationships...")
        print("=" * 60)
        
        try:
            # Get all users
            print("📥 Retrieving all users...")
            users_ref = self.db.collection('users')
            users_docs = list(users_ref.stream())
            users = [doc.to_dict() | {'_id': doc.id} for doc in users_docs]
            print(f"✅ Retrieved {len(users)} users")
            
            # Get all user_profiles (might contain school connections)
            print("📥 Retrieving all user profiles...")
            profiles_ref = self.db.collection('user_profiles')
            profiles_docs = list(profiles_ref.stream())
            user_profiles = [doc.to_dict() | {'_id': doc.id} for doc in profiles_docs]
            print(f"✅ Retrieved {len(user_profiles)} user profiles")
            
            # Analyze funder distribution
            funders = {}
            users_with_funders = 0
            users_with_schools = 0
            
            for user in users:
                funder = user.get('funder', None)
                if funder:
                    users_with_funders += 1
                    if funder not in funders:
                        funders[funder] = 0
                    funders[funder] += 1
                
                # Check if user has school connections
                schools = user.get('schools', {})
                if schools:
                    users_with_schools += 1
            
            print(f"\n📊 Funder Analysis:")
            print(f"  • Users with funders: {users_with_funders}")
            print(f"  • Users with schools: {users_with_schools}")
            print(f"  • Unique funders: {len(funders)}")
            
            if funders:
                print(f"\n💰 Funder Distribution:")
                for funder, count in sorted(funders.items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {funder}: {count} users")
            
            return users, user_profiles, funders
            
        except Exception as e:
            print(f"❌ Error analyzing user-school connections: {e}")
            return [], [], {}

    def create_comprehensive_funder_analysis_csv(self, output_filename: str = None):
        """
        Create a comprehensive CSV that connects users, funders, schools, teams, and team groups.
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"comprehensive_funder_analysis_{timestamp}.csv"
        
        print(f"\n📊 Creating comprehensive funder analysis CSV: {output_filename}")
        print("=" * 70)
        
        # Get all the data we need
        users, user_profiles, funders = self.analyze_user_school_connections()
        team_groups, teams = self.analyze_team_relationships()
        
        if not users or not teams:
            print("❌ Missing required data")
            return None
        
        # Create comprehensive analysis data
        analysis_data = []
        
        # Process each user and their connections
        for user in users:
            user_id = user.get('_id', '')
            user_email = user.get('email', '')
            user_funder = user.get('funder', 'No Funder')
            user_schools = user.get('schools', {})
            
            if user_schools and isinstance(user_schools, dict):
                # User has school connections
                for school_id, school_info in user_schools.items():
                    school_name = school_info.get('displayName', school_info.get('name', 'Unknown'))
                    school_city = school_info.get('city', '')
                    school_state = school_info.get('state', '')
                    school_ncessch = school_info.get('ncessch', school_id)
                    
                    # Find which team(s) this school belongs to
                    school_teams = []
                    for team in teams:
                        team_schools = team.get('schools', {})
                        if isinstance(team_schools, dict) and school_ncessch in team_schools:
                            school_teams.append(team)
                    
                    if school_teams:
                        # School is in one or more teams
                        for team in school_teams:
                            team_id = team.get('_id', '')
                            team_name = team.get('name', 'Unknown')
                            team_metadata = team.get('metadata', {})
                            
                            # Find which team group(s) this team belongs to
                            team_groups_for_team = []
                            for group in team_groups:
                                embedded_teams = group.get('teams', {})
                                if isinstance(embedded_teams, dict) and team_id in embedded_teams:
                                    team_groups_for_team.append(group)
                            
                            if team_groups_for_team:
                                # Team is in one or more team groups
                                for group in team_groups_for_team:
                                    analysis_data.append({
                                        'user_id': user_id,
                                        'user_email': user_email,
                                        'funder': user_funder,
                                        'school_id': school_ncessch,
                                        'school_name': school_name,
                                        'school_city': school_city,
                                        'school_state': school_state,
                                        'team_id': team_id,
                                        'team_name': team_name,
                                        'team_school_count': team_metadata.get('schoolCount', 0),
                                        'team_group_id': group.get('_id', ''),
                                        'team_group_name': group.get('name', 'Unknown'),
                                        'team_group_description': group.get('description', ''),
                                        'connection_status': 'fully_connected',
                                        'notes': 'User -> School -> Team -> Team Group'
                                    })
                            else:
                                # Team exists but not in any team group
                                analysis_data.append({
                                    'user_id': user_id,
                                    'user_email': user_email,
                                    'funder': user_funder,
                                    'school_id': school_ncessch,
                                    'school_name': school_name,
                                    'school_city': school_city,
                                    'school_state': school_state,
                                    'team_id': team_id,
                                    'team_name': team_name,
                                    'team_school_count': team_metadata.get('schoolCount', 0),
                                    'team_group_id': 'NONE',
                                    'team_group_name': 'UNREFERENCED_TEAM',
                                    'team_group_description': 'Team not in any group',
                                    'connection_status': 'team_ungrouped',
                                    'notes': 'User -> School -> Team (no group)'
                                })
                    else:
                        # School exists but not in any team
                        analysis_data.append({
                            'user_id': user_id,
                            'user_email': user_email,
                            'funder': user_funder,
                            'school_id': school_ncessch,
                            'school_name': school_name,
                            'school_city': school_city,
                            'school_state': school_state,
                            'team_id': 'NONE',
                            'team_name': 'SCHOOL_NOT_IN_TEAM',
                            'team_school_count': 0,
                            'team_group_id': 'NONE',
                            'team_group_name': 'SCHOOL_NOT_IN_TEAM',
                            'team_group_description': 'School not in any team',
                            'connection_status': 'school_orphaned',
                            'notes': 'User -> School (no team)'
                        })
            else:
                # User has no school connections
                analysis_data.append({
                    'user_id': user_id,
                    'user_email': user_email,
                    'funder': user_funder,
                    'school_id': 'NONE',
                    'school_name': 'NO_SCHOOLS',
                    'school_city': '',
                    'school_state': '',
                    'team_id': 'NONE',
                    'team_name': 'NO_SCHOOLS',
                    'team_school_count': 0,
                    'team_group_id': 'NONE',
                    'team_group_name': 'NO_SCHOOLS',
                    'team_group_description': 'User has no school connections',
                    'connection_status': 'user_no_schools',
                    'notes': 'User has no school connections'
                })
        
        # Create DataFrame and save to CSV
        if analysis_data:
            df = pd.DataFrame(analysis_data)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            # Calculate statistics
            total_users = len(set([r['user_id'] for r in analysis_data]))
            users_with_funders = len(set([r['user_id'] for r in analysis_data if r['funder'] != 'No Funder']))
            unique_funders = len(set([r['funder'] for r in analysis_data if r['funder'] != 'No Funder']))
            fully_connected = len([r for r in analysis_data if r['connection_status'] == 'fully_connected'])
            
            print(f"✅ Comprehensive funder analysis CSV created: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            print(f"📈 Funder Analysis Summary:")
            print(f"  • Total users: {total_users}")
            print(f"  • Users with funders: {users_with_funders}")
            print(f"  • Unique funders: {unique_funders}")
            print(f"  • Fully connected records: {fully_connected}")
            print(f"  • Connection statuses:")
            
            status_counts = {}
            for record in analysis_data:
                status = record['connection_status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            for status, count in sorted(status_counts.items()):
                print(f"    - {status}: {count}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None

    def create_funder_summary_csv(self, output_filename: str = None):
        """
        Create a summary CSV focused on funder statistics.
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"funder_summary_{timestamp}.csv"
        
        print(f"\n📊 Creating funder summary CSV: {output_filename}")
        print("=" * 50)
        
        # Get data
        users, user_profiles, funders = self.analyze_user_school_connections()
        team_groups, teams = self.analyze_team_relationships()
        
        if not users:
            print("❌ No user data available")
            return None
        
        # Create funder summary data
        funder_summary = []
        
        for funder_name, user_count in funders.items():
            # Find all users with this funder
            funder_users = [u for u in users if u.get('funder') == funder_name]
            
            # Count schools, teams, and team groups connected to this funder
            funder_schools = set()
            funder_teams = set()
            funder_team_groups = set()
            funder_states = set()
            
            for user in funder_users:
                user_schools = user.get('schools', {})
                if isinstance(user_schools, dict):
                    for school_id, school_info in user_schools.items():
                        school_ncessch = school_info.get('ncessch', school_id)
                        funder_schools.add(school_ncessch)
                        
                        # Add state
                        state = school_info.get('state', '')
                        if state:
                            funder_states.add(state)
                        
                        # Find teams for this school
                        for team in teams:
                            team_schools = team.get('schools', {})
                            if isinstance(team_schools, dict) and school_ncessch in team_schools:
                                funder_teams.add(team.get('_id', ''))
                                
                                # Find team groups for this team
                                for group in team_groups:
                                    embedded_teams = group.get('teams', {})
                                    if isinstance(embedded_teams, dict) and team.get('_id') in embedded_teams:
                                        funder_team_groups.add(group.get('_id', ''))
            
            funder_summary.append({
                'funder': funder_name,
                'total_users': user_count,
                'total_schools': len(funder_schools),
                'total_teams': len(funder_teams),
                'total_team_groups': len(funder_team_groups),
                'states_covered': len(funder_states),
                'state_list': ', '.join(sorted(funder_states)) if funder_states else '',
                'avg_schools_per_user': round(len(funder_schools) / user_count, 2) if user_count > 0 else 0
            })
        
        # Add summary for users without funders
        no_funder_users = [u for u in users if not u.get('funder')]
        if no_funder_users:
            no_funder_schools = set()
            no_funder_teams = set()
            no_funder_team_groups = set()
            no_funder_states = set()
            
            for user in no_funder_users:
                user_schools = user.get('schools', {})
                if isinstance(user_schools, dict):
                    for school_id, school_info in user_schools.items():
                        school_ncessch = school_info.get('ncessch', school_id)
                        no_funder_schools.add(school_ncessch)
                        
                        state = school_info.get('state', '')
                        if state:
                            no_funder_states.add(state)
                        
                        # Find teams for this school
                        for team in teams:
                            team_schools = team.get('schools', {})
                            if isinstance(team_schools, dict) and school_ncessch in team_schools:
                                no_funder_teams.add(team.get('_id', ''))
                                
                                # Find team groups for this team
                                for group in team_groups:
                                    embedded_teams = group.get('teams', {})
                                    if isinstance(embedded_teams, dict) and team.get('_id') in embedded_teams:
                                        no_funder_team_groups.add(group.get('_id', ''))
            
            funder_summary.append({
                'funder': 'NO_FUNDER',
                'total_users': len(no_funder_users),
                'total_schools': len(no_funder_schools),
                'total_teams': len(no_funder_teams),
                'total_team_groups': len(no_funder_team_groups),
                'states_covered': len(no_funder_states),
                'state_list': ', '.join(sorted(no_funder_states)) if no_funder_states else '',
                'avg_schools_per_user': round(len(no_funder_schools) / len(no_funder_users), 2) if no_funder_users else 0
            })
        
        # Create DataFrame and save to CSV
        if funder_summary:
            df = pd.DataFrame(funder_summary)
            df = df.sort_values('total_schools', ascending=False)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            print(f"✅ Funder summary CSV created: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None

    def explore_user_profiles_structure(self, sample_size: int = 3):
        """
        Explore the structure of the user_profiles collection to understand school connections.
        
        Args:
            sample_size: Number of sample documents to analyze
        """
        print(f"\n🔍 Exploring 'user_profiles' collection structure:")
        print("=" * 50)
        
        try:
            # Get sample documents
            profiles_ref = self.db.collection('user_profiles')
            docs = list(profiles_ref.limit(sample_size).stream())
            
            if not docs:
                print("❌ No documents found in user_profiles collection")
                return []
            
            print(f"📊 Found {len(docs)} sample documents")
            
            # Analyze structure
            all_fields = {}
            
            for doc in docs:
                data = doc.to_dict()
                data['_id'] = doc.id
                
                for field, value in data.items():
                    if field not in all_fields:
                        all_fields[field] = []
                    
                    # Store sample values (first 2 per field)
                    if len(all_fields[field]) < 2:
                        if isinstance(value, dict):
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}...")
                        elif isinstance(value, list):
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}...")
                        else:
                            all_fields[field].append(f"{type(value).__name__}: {str(value)[:100]}")
            
            # Print field analysis
            print(f"\n📋 Field analysis:")
            for field, samples in sorted(all_fields.items()):
                sample_str = " | ".join(samples)
                print(f"  • {field:<20} ({samples[0].split(':')[0]}): {sample_str}")
            
            # Print detailed sample document
            if docs:
                sample_doc = docs[0].to_dict()
                sample_doc['_id'] = docs[0].id
                print(f"\n📄 Sample document:")
                for key, value in sample_doc.items():
                    if isinstance(value, dict):
                        print(f"  {key}: {str(value)[:200]}...")
                    elif isinstance(value, list):
                        print(f"  {key}: {str(value)[:200]}...")
                    else:
                        print(f"  {key}: {value}")
            
            return [doc.to_dict() | {'_id': doc.id} for doc in docs]
            
        except Exception as e:
            print(f"❌ Error exploring user_profiles collection: {e}")
            return []

    def analyze_user_school_connections_v2(self):
        """
        Enhanced analysis of user-school connections using user_profiles and funder relationships.
        """
        print(f"\n🔗 Analyzing user-school connections via user_profiles...")
        print("=" * 60)
        
        try:
            # Get all users with funder info
            print("📥 Retrieving all users...")
            users_ref = self.db.collection('users')
            users_docs = list(users_ref.stream())
            users = [doc.to_dict() | {'_id': doc.id} for doc in users_docs]
            print(f"✅ Retrieved {len(users)} users")
            
            # Get all user_profiles (likely contains school connections)
            print("📥 Retrieving all user profiles...")
            profiles_ref = self.db.collection('user_profiles')
            profiles_docs = list(profiles_ref.stream())
            user_profiles = [doc.to_dict() | {'_id': doc.id} for doc in profiles_docs]
            print(f"✅ Retrieved {len(user_profiles)} user profiles")
            
            # Create user lookup by ID for funder information
            user_funder_lookup = {}
            funders = {}
            users_with_funders = 0
            
            for user in users:
                user_id = user.get('_id', '')
                funder = user.get('funder', None)
                user_funder_lookup[user_id] = funder
                
                if funder:
                    users_with_funders += 1
                    if funder not in funders:
                        funders[funder] = 0
                    funders[funder] += 1
            
            # Analyze user_profiles for school connections
            profiles_with_schools = 0
            
            for profile in user_profiles:
                schools = profile.get('schools', {})
                if schools:
                    profiles_with_schools += 1
            
            print(f"\n📊 Connection Analysis:")
            print(f"  • Users with funders: {users_with_funders}")
            print(f"  • User profiles with schools: {profiles_with_schools}")
            print(f"  • Unique funders: {len(funders)}")
            
            if funders:
                print(f"\n💰 Funder Distribution:")
                for funder, count in sorted(funders.items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {funder}: {count} users")
            
            return users, user_profiles, funders, user_funder_lookup
            
        except Exception as e:
            print(f"❌ Error analyzing user-school connections: {e}")
            return [], [], {}, {}

    def create_final_funder_analysis_csv(self, output_filename: str = None):
        """
        Create the definitive CSV that connects users with funders to teams via user_profiles,
        then shows all schools within those teams and their team groups.
        
        Connection chain: Users (funders) → User_Profiles (teams) → Teams (schools) → Team Groups
        
        Args:
            output_filename: Name of the output CSV file
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"final_funder_analysis_{timestamp}.csv"
        
        print(f"\n📊 Creating FINAL comprehensive funder analysis CSV: {output_filename}")
        print("=" * 80)
        
        # Get all the data we need
        users, user_profiles, funders, user_funder_lookup = self.analyze_user_school_connections_v2()
        team_groups, teams = self.analyze_team_relationships()
        
        if not users or not teams:
            print("❌ Missing required data")
            return None
        
        # Create comprehensive analysis data
        analysis_data = []
        
        # Process each user_profile and connect through teams to schools
        for profile in user_profiles:
            profile_id = profile.get('_id', '')
            user_id = profile.get('_id', '')  # In this case, profile ID seems to match user ID
            user_funder = user_funder_lookup.get(user_id, 'No Funder')
            profile_teams = profile.get('teams', {})
            
            # Get user info if available
            user_info = next((u for u in users if u.get('_id') == user_id), {})
            user_email = user_info.get('email', profile.get('email', 'Unknown'))
            user_name = user_info.get('name', profile.get('name', 'Unknown'))
            user_department = profile.get('department', '')
            user_role = profile.get('role', '')
            
            if profile_teams and isinstance(profile_teams, dict):
                # Profile has team connections - find schools through teams
                for team_id, team_info in profile_teams.items():
                    team_name_in_profile = team_info.get('name', 'Unknown')
                    user_role_in_team = team_info.get('role', '')
                    user_added_to_team = team_info.get('added', '')
                    
                    # Find the full team document
                    full_team = next((t for t in teams if t.get('_id') == team_id), None)
                    
                    if full_team:
                        team_full_name = full_team.get('name', 'Unknown')
                        team_metadata = full_team.get('metadata', {})
                        team_schools = full_team.get('schools', {})
                        
                        # Find which team group(s) this team belongs to
                        team_groups_for_team = []
                        for group in team_groups:
                            embedded_teams = group.get('teams', {})
                            if isinstance(embedded_teams, dict) and team_id in embedded_teams:
                                team_groups_for_team.append(group)
                        
                        if team_schools and isinstance(team_schools, dict):
                            # Create a record for each school in the team
                            for school_id, school_info in team_schools.items():
                                school_name = school_info.get('displayName', 'Unknown')
                                school_city = school_info.get('city', '')
                                school_state = school_info.get('state', '')
                                school_ncessch = school_info.get('ncessch', school_id)
                                
                                if team_groups_for_team:
                                    # Team is in one or more team groups
                                    for group in team_groups_for_team:
                                        analysis_data.append({
                                            'user_id': user_id,
                                            'user_email': user_email,
                                            'user_name': user_name,
                                            'user_department': user_department,
                                            'user_role': user_role,
                                            'funder': user_funder,
                                            'team_id': team_id,
                                            'team_name_in_profile': team_name_in_profile,
                                            'team_full_name': team_full_name,
                                            'user_role_in_team': user_role_in_team,
                                            'user_added_to_team': str(user_added_to_team),
                                            'team_school_count': team_metadata.get('schoolCount', 0),
                                            'team_created': str(team_metadata.get('created', '')),
                                            'school_id': school_ncessch,
                                            'school_name': school_name,
                                            'school_city': school_city,
                                            'school_state': school_state,
                                            'team_group_id': group.get('_id', ''),
                                            'team_group_name': group.get('name', 'Unknown'),
                                            'team_group_description': group.get('description', ''),
                                            'connection_status': 'fully_connected',
                                            'connection_chain': 'User → User_Profile → Team → School → Team_Group'
                                        })
                                else:
                                    # Team exists but not in any team group
                                    analysis_data.append({
                                        'user_id': user_id,
                                        'user_email': user_email,
                                        'user_name': user_name,
                                        'user_department': user_department,
                                        'user_role': user_role,
                                        'funder': user_funder,
                                        'team_id': team_id,
                                        'team_name_in_profile': team_name_in_profile,
                                        'team_full_name': team_full_name,
                                        'user_role_in_team': user_role_in_team,
                                        'user_added_to_team': str(user_added_to_team),
                                        'team_school_count': team_metadata.get('schoolCount', 0),
                                        'team_created': str(team_metadata.get('created', '')),
                                        'school_id': school_ncessch,
                                        'school_name': school_name,
                                        'school_city': school_city,
                                        'school_state': school_state,
                                        'team_group_id': 'NONE',
                                        'team_group_name': 'UNREFERENCED_TEAM',
                                        'team_group_description': 'Team not in any group',
                                        'connection_status': 'team_ungrouped',
                                        'connection_chain': 'User → User_Profile → Team → School (no group)'
                                    })
                        else:
                            # Team exists but has no schools
                            if team_groups_for_team:
                                for group in team_groups_for_team:
                                    analysis_data.append({
                                        'user_id': user_id,
                                        'user_email': user_email,
                                        'user_name': user_name,
                                        'user_department': user_department,
                                        'user_role': user_role,
                                        'funder': user_funder,
                                        'team_id': team_id,
                                        'team_name_in_profile': team_name_in_profile,
                                        'team_full_name': team_full_name,
                                        'user_role_in_team': user_role_in_team,
                                        'user_added_to_team': str(user_added_to_team),
                                        'team_school_count': team_metadata.get('schoolCount', 0),
                                        'team_created': str(team_metadata.get('created', '')),
                                        'school_id': 'NONE',
                                        'school_name': 'TEAM_HAS_NO_SCHOOLS',
                                        'school_city': '',
                                        'school_state': '',
                                        'team_group_id': group.get('_id', ''),
                                        'team_group_name': group.get('name', 'Unknown'),
                                        'team_group_description': group.get('description', ''),
                                        'connection_status': 'team_no_schools',
                                        'connection_chain': 'User → User_Profile → Team (no schools) → Team_Group'
                                    })
                            else:
                                analysis_data.append({
                                    'user_id': user_id,
                                    'user_email': user_email,
                                    'user_name': user_name,
                                    'user_department': user_department,
                                    'user_role': user_role,
                                    'funder': user_funder,
                                    'team_id': team_id,
                                    'team_name_in_profile': team_name_in_profile,
                                    'team_full_name': team_full_name,
                                    'user_role_in_team': user_role_in_team,
                                    'user_added_to_team': str(user_added_to_team),
                                    'team_school_count': team_metadata.get('schoolCount', 0),
                                    'team_created': str(team_metadata.get('created', '')),
                                    'school_id': 'NONE',
                                    'school_name': 'TEAM_HAS_NO_SCHOOLS',
                                    'school_city': '',
                                    'school_state': '',
                                    'team_group_id': 'NONE',
                                    'team_group_name': 'UNREFERENCED_TEAM',
                                    'team_group_description': 'Team not in any group',
                                    'connection_status': 'team_ungrouped_no_schools',
                                    'connection_chain': 'User → User_Profile → Team (no schools, no group)'
                                })
                    else:
                        # Team referenced in profile but not found in teams collection
                        analysis_data.append({
                            'user_id': user_id,
                            'user_email': user_email,
                            'user_name': user_name,
                            'user_department': user_department,
                            'user_role': user_role,
                            'funder': user_funder,
                            'team_id': team_id,
                            'team_name_in_profile': team_name_in_profile,
                            'team_full_name': 'TEAM_NOT_FOUND',
                            'user_role_in_team': user_role_in_team,
                            'user_added_to_team': str(user_added_to_team),
                            'team_school_count': 0,
                            'team_created': '',
                            'school_id': 'NONE',
                            'school_name': 'TEAM_NOT_FOUND',
                            'school_city': '',
                            'school_state': '',
                            'team_group_id': 'NONE',
                            'team_group_name': 'TEAM_NOT_FOUND',
                            'team_group_description': '',
                            'connection_status': 'team_not_found',
                            'connection_chain': 'User → User_Profile → Team (not found)'
                        })
            else:
                # Profile has no team connections
                analysis_data.append({
                    'user_id': user_id,
                    'user_email': user_email,
                    'user_name': user_name,
                    'user_department': user_department,
                    'user_role': user_role,
                    'funder': user_funder,
                    'team_id': 'NONE',
                    'team_name_in_profile': 'NO_TEAMS',
                    'team_full_name': 'NO_TEAMS',
                    'user_role_in_team': '',
                    'user_added_to_team': '',
                    'team_school_count': 0,
                    'team_created': '',
                    'school_id': 'NONE',
                    'school_name': 'NO_TEAMS',
                    'school_city': '',
                    'school_state': '',
                    'team_group_id': 'NONE',
                    'team_group_name': 'NO_TEAMS',
                    'team_group_description': '',
                    'connection_status': 'user_no_teams',
                    'connection_chain': 'User → User_Profile (no teams)'
                })
        
        # Create DataFrame and save to CSV
        if analysis_data:
            df = pd.DataFrame(analysis_data)
            
            # Ensure output directory exists
            os.makedirs('output', exist_ok=True)
            output_path = os.path.join('output', output_filename)
            
            df.to_csv(output_path, index=False)
            
            # Calculate comprehensive statistics
            total_users = len(set([r['user_id'] for r in analysis_data]))
            users_with_funders = len(set([r['user_id'] for r in analysis_data if r['funder'] != 'No Funder']))
            unique_funders = len(set([r['funder'] for r in analysis_data if r['funder'] != 'No Funder']))
            users_with_teams = len(set([r['user_id'] for r in analysis_data if r['team_id'] != 'NONE']))
            total_schools = len(set([r['school_id'] for r in analysis_data if r['school_id'] != 'NONE']))
            fully_connected = len([r for r in analysis_data if r['connection_status'] == 'fully_connected'])
            unique_teams = len(set([r['team_id'] for r in analysis_data if r['team_id'] != 'NONE']))
            unique_team_groups = len(set([r['team_group_id'] for r in analysis_data if r['team_group_id'] != 'NONE']))
            states_covered = len(set([r['school_state'] for r in analysis_data if r['school_state']]))
            
            print(f"✅ FINAL comprehensive funder analysis CSV created: {output_path}")
            print(f"📊 Total rows: {len(df)}")
            print(f"🎯 COMPREHENSIVE FUNDER ANALYSIS SUMMARY:")
            print(f"  👥 Users:")
            print(f"    • Total users: {total_users}")
            print(f"    • Users with funders: {users_with_funders}")
            print(f"    • Users with teams: {users_with_teams}")
            print(f"  💰 Funding:")
            print(f"    • Unique funders: {unique_funders}")
            print(f"  🏫 Educational Infrastructure:")
            print(f"    • Total schools reached: {total_schools}")
            print(f"    • Unique teams: {unique_teams}")
            print(f"    • Unique team groups: {unique_team_groups}")
            print(f"    • States covered: {states_covered}")
            print(f"  🔗 Connections:")
            print(f"    • Fully connected records: {fully_connected}")
            print(f"  📊 Connection statuses:")
            
            status_counts = {}
            for record in analysis_data:
                status = record['connection_status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            for status, count in sorted(status_counts.items()):
                print(f"    - {status}: {count}")
            
            return output_path
        else:
            print("❌ No data to write to CSV")
            return None


def main():
    """
    Main function to run the comprehensive Firebase analysis with funder information.
    """
    print("🔥 Firebase Comprehensive Analysis: Users, Funders, Schools, Teams & Groups")
    print("=" * 80)
    
    # Initialize analyzer
    analyzer = FirebaseAnalysis()
    
    # First, explore the users structure to understand funder connections
    print("🔍 Step 1: Exploring Users Collection Structure...")
    analyzer.explore_users_structure()
    
    # Create comprehensive funder analysis
    print("\n💰 Step 2: Creating Comprehensive Funder Analysis...")
    funder_comprehensive_path = analyzer.create_comprehensive_funder_analysis_csv()
    
    # Create funder summary
    print("\n📊 Step 3: Creating Funder Summary...")
    funder_summary_path = analyzer.create_funder_summary_csv()
    
    # Create the detailed team-school analysis CSV
    print("\n🏫 Step 4: Creating Detailed Team-School Analysis...")
    detailed_path = analyzer.create_detailed_team_school_analysis_csv()
    
    # Create the school summary CSV
    print("\n📈 Step 5: Creating School Summary...")
    summary_path = analyzer.create_school_summary_csv()
    
    # Create the original team analysis CSV for comparison
    print("\n🔗 Step 6: Creating Original Team Analysis...")
    original_path = analyzer.create_team_analysis_csv()
    
    print("\n" + "=" * 80)
    print("📋 Complete Analysis Results:")
    print("💰 FUNDER ANALYSIS:")
    if funder_comprehensive_path:
        print(f"  ✅ Comprehensive funder analysis: {funder_comprehensive_path}")
    if funder_summary_path:
        print(f"  ✅ Funder summary: {funder_summary_path}")
    
    print("\n🏫 SCHOOL & TEAM ANALYSIS:")
    if detailed_path:
        print(f"  ✅ Detailed team-school analysis: {detailed_path}")
    if summary_path:
        print(f"  ✅ School summary by team groups: {summary_path}")
    if original_path:
        print(f"  ✅ Original team analysis: {original_path}")
    
    files_created = [f for f in [funder_comprehensive_path, funder_summary_path, detailed_path, summary_path, original_path] if f]
    
    if files_created:
        print(f"\n🎉 Analysis complete! {len(files_created)} files generated successfully.")
        print("\n🔗 Connection Chain Analysis:")
        print("  Users (with funders) → Schools → Teams → Team Groups")
        print("  This provides complete traceability from funding sources to educational outcomes!")
    else:
        print("\n❌ Analysis failed")


if __name__ == "__main__":
    main() 