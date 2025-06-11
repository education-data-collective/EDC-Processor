from flask import current_app
from app import db
from app.models import School, DirectoryEntry, MembershipData, FrlData
from sqlalchemy.orm import scoped_session, sessionmaker
from app.utils.admin_helpers import (
    get_lowest_highest_grades, 
    get_school_level, 
    get_grade_range, 
    format_grades_offered, 
    get_split_proportions,
    convert_grades_to_db_format
)
from firebase_admin import firestore
from app.unified_processing.utils import update_overall_status

def get_db_session(bind_key):
    """Get a scoped session for the specified database"""
    engine = db.get_engine(bind=bind_key)
    Session = scoped_session(sessionmaker(bind=engine))
    return Session()


def validate_split_config(split):
    """Validate a single split configuration"""
    required_fields = ['ncessch', 'displayName', 'gradesServed', 'address']
    missing_fields = [f for f in required_fields if f not in split]
    if missing_fields:
        raise ValueError(f"Split missing required fields: {', '.join(missing_fields)}")

    address = split.get('address')
    required_address_fields = ['street', 'city', 'state', 'zip']
    if not all(k in address for k in required_address_fields):
        raise ValueError(f"Invalid address configuration for split {split['ncessch']}")

    if not isinstance(split['gradesServed'], list):
        raise ValueError(f"gradesServed must be a list for split {split['ncessch']}")

    return True

def process_nces_update(payload, session=None):
    """Process NCES database updates for a single split school"""
    
    try:
        # Use the correct bind for NCES data operations
        if not session:
            session = db.session

        ncessch = payload.get('ncessch')
        parent_ncessch = payload.get('parent_ncessch')
        split_data = payload.get('split_data')
        
        if not all([ncessch, parent_ncessch, split_data]):
            raise ValueError("Missing required payload data")

        # Initialize Firebase for split school status
        firestore_db = firestore.client()
        status_ref = (firestore_db.collection('schools')
                    .document(ncessch)
                    .collection('processing_status')
                    .document('current'))

        # Update status to in_progress
        status_ref.update({
            'stages.nces_data': {
                'status': 'in_progress',
                'updated_at': firestore.SERVER_TIMESTAMP
            }
        })

        current_app.logger.info(f"Processing NCES updates for split school {ncessch}")
        current_app.logger.debug(f"Split data received: {split_data}")
        current_app.logger.info(f"Split data keys available: {list(split_data.keys())}")

        # Get parent school using the session
        parent_school = session.query(School).filter_by(ncessch=parent_ncessch).first()
        if not parent_school:
            raise ValueError(f"Parent school not found: {parent_ncessch}")
        
        current_app.logger.info(f"Parent school found: {parent_school.id}")

        try:
            current_app.logger.info(f"Beginning NCES update for {ncessch}")
            with session.begin_nested():
                # Check if school already exists
                existing_school = session.query(School).filter_by(ncessch=ncessch).first()
                if existing_school:
                    current_app.logger.info(f"School {ncessch} already exists, clearing existing records")
                    session.query(DirectoryEntry).filter_by(school_id=existing_school.id).delete()
                    session.query(MembershipData).filter_by(school_id=existing_school.id).delete()
                    session.query(FrlData).filter_by(school_id=existing_school.id).delete()
                    school_id = existing_school.id
                else:
                    # Create new school record
                    new_school = School(
                        ncessch=ncessch,
                        school_name=split_data['school_name'],
                        lea_name=parent_school.lea_name,
                        state_name=parent_school.state_name,
                        state_abbr=parent_school.state_abbr
                    )
                    session.add(new_school)
                    session.flush()
                    school_id = new_school.id

                proportion = get_split_proportions(
                    parent_school.id, 
                    split_data['grades_served']
                )

                current_app.logger.info(f"Split proportions for {ncessch}: {proportion}")

                # Create directory entries
                current_app.logger.info(f"Creating directory entries for {ncessch}")

                # Convert grades before using them
                formatted_grades = convert_grades_to_db_format(split_data['grades_served'])

                for year in [2019, 2023]:
                    parent_entry = session.query(DirectoryEntry).filter_by(
                        school_id=parent_school.id,
                        data_year=year
                    ).first()

                    if parent_entry:
                        # Use formatted grades in all grade-related functions
                        grades_offered = format_grades_offered(formatted_grades)
                        lowest_grade, highest_grade = get_lowest_highest_grades(formatted_grades)
                        level = get_school_level(formatted_grades)
                        grade_range = get_grade_range(formatted_grades)

                        new_dir = DirectoryEntry(
                            school_id=school_id,
                            data_year=year,
                            street_address=split_data['street'],
                            city=split_data['city'],
                            state=split_data['state'],
                            zip_code=split_data['zip'],
                            latitude=split_data['address'].get('latitude'),
                            longitude=split_data['address'].get('longitude'),
                            status=parent_entry.status,
                            school_type=parent_entry.school_type,
                            charter=parent_entry.charter,
                            grades_offered=grades_offered,
                            lowest_grade=lowest_grade,
                            highest_grade=highest_grade,
                            level=level,
                            grade_range=grade_range,
                            teachers=round(parent_entry.teachers * proportion, 1) if parent_entry.teachers else None
                        )
                        session.add(new_dir)

                # Split membership data - using grades_served consistently
                current_app.logger.info(f"Original grades: {split_data['grades_served']}")
                current_app.logger.info(f"Formatted grades: {formatted_grades}")
                current_app.logger.info(f"Existing membership grades: {[row.grade for row in session.query(MembershipData.grade).distinct().all()]}")
                membership_rows = session.query(MembershipData).filter(
                    MembershipData.school_id == parent_school.id,
                    MembershipData.grade.in_(formatted_grades)
                ).all()

                current_app.logger.info(f"Found {len(membership_rows)} membership rows")
                
                for row in membership_rows:
                    new_membership = MembershipData(
                        school_id=school_id,
                        data_year=row.data_year,
                        school_year=row.school_year,
                        grade=row.grade,
                        total_membership=row.total_membership,
                        american_indian=row.american_indian,
                        asian=row.asian,
                        black=row.black,
                        hispanic=row.hispanic,
                        pacific_islander=row.pacific_islander,
                        white=row.white,
                        two_or_more_races=row.two_or_more_races
                    )
                    session.add(new_membership)

                # Split FRL data
                frl_rows = session.query(FrlData).filter_by(
                    school_id=parent_school.id
                ).all()

                for row in frl_rows:
                    new_frl = FrlData(
                        school_id=school_id,
                        data_year=row.data_year,
                        school_year=row.school_year,
                        frl_count=int(row.frl_count * proportion) if row.frl_count else 0,
                        dms_flag=row.dms_flag
                    )
                    session.add(new_frl)

            # Commit the transaction
            session.commit()

            # Update success status
            status_ref.update({
                'stages.nces_data': {
                    'status': 'completed',
                    'updated_at': firestore.SERVER_TIMESTAMP,
                    'details': f"Successfully processed split school {ncessch}"
                }
            })

            # Update overall status
            status_doc = status_ref.get()
            if status_doc.exists:
                current_data = status_doc.to_dict()
                current_stages = current_data.get('stages', {})
                update_overall_status(status_ref, current_stages)

            return {
                'status': 'success',
                'ncessch': ncessch
            }

        except Exception as e:
            if session:
                session.rollback()
            raise

    except Exception as e:
        current_app.logger.error(f"NCES update error for {ncessch}: {str(e)}")
        try:
            status_ref.update({
                'stages.nces_data': {
                    'status': 'failed',
                    'error': str(e),
                    'updated_at': firestore.SERVER_TIMESTAMP
                }
            })
            # Update overall status
            status_doc = status_ref.get()
            if status_doc.exists:
                current_data = status_doc.to_dict()
                current_stages = current_data.get('stages', {})
                update_overall_status(status_ref, current_stages)
        except Exception as firebase_error:
            current_app.logger.error(f"Failed to update Firebase status: {str(firebase_error)}")
        return {
            'status': 'error',
            'error': str(e)
        }
    finally:
        if session:
            session.remove()