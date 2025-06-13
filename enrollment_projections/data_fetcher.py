import logging
from flask import current_app
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_db_engine(db_type='nces'):
    if db_type == 'nces':
        db_uri = current_app.config['SQLALCHEMY_BINDS']['nces_data']
    elif db_type == 'esri':
        db_uri = current_app.config['SQLALCHEMY_BINDS']['esri_data']
    else:
        raise ValueError(f"Unknown database type: {db_type}")
    
    logger.debug(f"Connecting to {db_type} database with URI: {db_uri}")
    return create_engine(db_uri)

def get_school_id_from_ncessch(session, ncessch):
    query = text("SELECT id FROM schools WHERE ncessch = :ncessch")
    result = session.execute(query, {"ncessch": ncessch}).fetchone()
    if result:
        return result[0]
    return None

def fetch_historical_data(ncessch: str):
    engine = get_db_engine('nces')
    Session = sessionmaker(bind=engine)

    with Session() as session:
        logger.debug(f"Fetching school ID for NCESSCH: {ncessch}")
        school_id = get_school_id_from_ncessch(session, ncessch)
        logger.debug(f"Retrieved school ID: {school_id}")

        if school_id is None:
            logger.debug(f"No school found for NCESSCH: {ncessch}")
            return []

        query = text("""
            SELECT m.school_id, m.school_year, m.grade, m.total_membership as total_enrollment,
                   'actual' as type
            FROM membership_data m
            WHERE m.school_id = :school_id
            ORDER BY m.school_year, m.grade
        """)

        logger.debug(f"Executing query for school_id: {school_id}")
        try:
            result = session.execute(query, {"school_id": school_id}).fetchall()
            logger.debug(f"Query executed successfully. Number of rows: {len(result)}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

        historical_data = [
            {
                "school_id": row.school_id,
                "school_year": row.school_year,
                "grade": row.grade,
                "total_enrollment": row.total_enrollment,
                "type": row.type
            }
            for row in result
        ]

        logger.debug(f"Processed {len(historical_data)} records for school_id: {school_id}")

    return historical_data

def fetch_school_info(ncessch: str):
    engine = get_db_engine('nces')
    Session = sessionmaker(bind=engine)

    with Session() as session:
        query = text("""
            SELECT id, ncessch, school_name, lea_name, state_name, state_abbr
            FROM schools
            WHERE ncessch = :ncessch
        """)

        logger.debug(f"Executing query for NCESSCH: {ncessch}")
        try:
            result = session.execute(query, {"ncessch": ncessch}).fetchone()
            logger.debug("Query executed successfully")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

        if result:
            school_info = {
                "id": result.id,
                "ncessch": result.ncessch,
                "school_name": result.school_name,
                "lea_name": result.lea_name,
                "state_name": result.state_name,
                "state_abbr": result.state_abbr,
            }
            logger.debug(f"Retrieved school info for NCESSCH: {ncessch}")
        else:
            school_info = None
            logger.debug(f"No school info found for NCESSCH: {ncessch}")

    return school_info