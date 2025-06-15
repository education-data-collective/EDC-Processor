from app import db
from app.models import School, DirectoryEntry, EsriData, SchoolPolygonRelationship
from firebase_admin import firestore
from shapely.geometry import Point, Polygon
from .fetch import fetch_esri_data
from app.utils.geocoding import GeocodingService
import json
import logging
from flask import current_app
import gc
from datetime import datetime
from sqlalchemy import extract, select, and_, func, delete, text
from datetime import datetime, timedelta



# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_esri_db():
    """Get the ESRI database engine"""
    try:
        return get_db_engine('esri_data')
    except Exception as e:
        current_app.logger.error(f"Error getting ESRI database: {str(e)}", exc_info=True)
        raise

def get_db_engine(bind_key):
    """Get the correct database engine based on environment"""
    try:
        # First try the bind parameter (works in some environments)
        return db.get_engine(bind=bind_key)
    except TypeError:
        # If that fails, try accessing the binds directly
        try:
            return db.engines[bind_key]
        except (KeyError, AttributeError):
            # If that fails, try getting from binds
            if hasattr(db, '_engines'):
                return db._engines.get(bind_key)
            # If all else fails, return default engine
            return db.engine

# Update the verification functions to use this new method
def verify_nces_connection():
    try:
        engine = get_db_engine('nces_data')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM schools")).scalar()
            current_app.logger.info(f"Total schools in database: {result}")
            
            test_ncessch = '110011700546'
            sql_result = conn.execute(
                text("SELECT * FROM schools WHERE ncessch = :ncessch"),
                {"ncessch": test_ncessch}
            ).first()
            current_app.logger.info(f"SQL Query result for {test_ncessch}: {sql_result}")
            
            school = School.query.filter_by(ncessch=test_ncessch).first()
            current_app.logger.info(f"SQLAlchemy result: {school}")
            
    except Exception as e:
        current_app.logger.error(f"Error in verification: {str(e)}", exc_info=True)
        raise  # We want to raise here to catch db connection issues early
        
    return None

def verify_esri_connection():
    try:
        engine = get_db_engine('esri_data')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM esri_data")).scalar()
            current_app.logger.info(f"Total ESRI records in database: {result}")
            
            # Test query with a specific ncessch
            test_ncessch = '110011700546'
            sql_result = conn.execute(
                text("SELECT COUNT(*) FROM esri_data WHERE ncessch = :ncessch"),
                {"ncessch": test_ncessch}
            ).scalar()
            current_app.logger.info(f"ESRI records for {test_ncessch}: {sql_result}")
            
    except Exception as e:
        current_app.logger.error(f"Error in ESRI DB verification: {str(e)}", exc_info=True)
        raise  # We want to raise here since ESRI connection is critical
        
    return None

def ensure_sequence_alignment():
    """Ensure database sequences are properly aligned with max IDs"""
    try:
        engine = get_db_engine('esri_data')
        with engine.begin() as conn:
            # Check and fix esri_data sequence
            max_id = conn.execute(text("""
                SELECT COALESCE(MAX(id), 0) FROM esri_data
            """)).scalar()
            
            current_seq = conn.execute(text("""
                SELECT last_value FROM esri_data_id_seq
            """)).scalar()
            
            if current_seq <= max_id:
                conn.execute(text("""
                    SELECT setval('esri_data_id_seq', :new_val, false)
                """), {'new_val': max_id + 1})
                current_app.logger.info(f"Reset esri_data sequence to {max_id + 1}")
                
            # Check and fix polygon relationships sequence
            max_id = conn.execute(text("""
                SELECT COALESCE(MAX(id), 0) FROM school_polygon_relationships
            """)).scalar()
            
            current_seq = conn.execute(text("""
                SELECT last_value FROM school_polygon_relationships_id_seq
            """)).scalar()
            
            if current_seq <= max_id:
                conn.execute(text("""
                    SELECT setval('school_polygon_relationships_id_seq', :new_val, false)
                """), {'new_val': max_id + 1})
                current_app.logger.info(f"Reset polygon relationships sequence to {max_id + 1}")
                
    except Exception as e:
        current_app.logger.error(f"Error aligning sequences: {str(e)}")
        raise

def get_nearby_schools_for_year(year, base_lat, base_lon, radius_degrees=0.25):
   """Get schools within a geographic radius for a specific year"""
   try:
       current_app.logger.info(f"Starting nearby schools query for year {year}")
       current_app.logger.debug(f"Search parameters: lat={base_lat}, lon={base_lon}, radius={radius_degrees}")
       
       lat_min = base_lat - radius_degrees
       lat_max = base_lat + radius_degrees
       lon_min = base_lon - radius_degrees
       lon_max = base_lon + radius_degrees
       
       with db.session.no_autoflush:
           subquery = (
               select(DirectoryEntry.school_id, func.max(DirectoryEntry.data_year).label('max_year'))
               .where(DirectoryEntry.data_year == year)
               .group_by(DirectoryEntry.school_id)
               .subquery()
           )
           
           schools_query = (
               select(School, DirectoryEntry)
               .join(DirectoryEntry, School.id == DirectoryEntry.school_id)
               .join(subquery, and_(
                   School.id == subquery.c.school_id,
                   DirectoryEntry.data_year == subquery.c.max_year
               ))
               .where(and_(
                   DirectoryEntry.latitude.between(lat_min, lat_max),
                   DirectoryEntry.longitude.between(lon_min, lon_max),
                   DirectoryEntry.latitude.isnot(None),
                   DirectoryEntry.longitude.isnot(None)
               ))
           )
           
           current_app.logger.debug(f"Executing query: {schools_query}")
           results = db.session.execute(schools_query).unique().fetchall()
           
           result_count = len(results)
           current_app.logger.info(f"Found {result_count} unique schools for year {year} within search radius")
           
           schools_dict = {school: directory_entry for school, directory_entry in results}
           current_app.logger.debug(f"Created dictionary with {len(schools_dict)} entries")
           
           return schools_dict
           
   except Exception as e:
       current_app.logger.error(f"Error fetching schools for year {year}: {str(e)}", exc_info=True)
       return {}
   
def check_id_not_exists(conn, id_to_check):
    """Check if an ID already exists in the table"""
    check_existing = text("""
        SELECT EXISTS(
            SELECT 1 
            FROM school_polygon_relationships 
            WHERE id = :id
        )
    """)
    return not conn.execute(check_existing, {'id': id_to_check}).scalar()

def get_valid_sequence_id(conn):
    """Get next sequence value and verify it's safe to use"""
    get_next_id = text("SELECT nextval('school_polygon_relationships_id_seq')")
    
    # Try up to 3 times to get a valid ID
    for attempt in range(3):
        next_id = conn.execute(get_next_id).scalar()
        current_app.logger.debug(f"Got sequence value: {next_id}")
        
        if check_id_not_exists(conn, next_id):
            return next_id
        
        current_app.logger.warning(f"Sequence value {next_id} already exists, incrementing sequence")
        # Increment sequence by a larger amount to try to get past any gaps
        conn.execute(text("SELECT setval('school_polygon_relationships_id_seq', :next_val)"), 
                    {'next_val': next_id + 100})
    
    # If we still can't get a valid ID, check max and reset sequence
    max_id = conn.execute(text("SELECT MAX(id) FROM school_polygon_relationships")).scalar()
    new_start = max_id + 1
    current_app.logger.info(f"Resetting sequence to start from {new_start}")
    conn.execute(text("ALTER SEQUENCE school_polygon_relationships_id_seq RESTART WITH :new_start"), 
                {'new_start': new_start})
    
    return conn.execute(get_next_id).scalar()

def update_polygon_relationships(ncessch, esri_data):
    # Ensure sequences are aligned first
    ensure_sequence_alignment()
    """Update polygon relationships for a school"""
    try:
        current_app.logger.info(f"Starting polygon relationships update for {ncessch}")
        
        # Run database verification first
        current_app.logger.info("Verifying database connections")
        verify_nces_connection()
        verify_esri_connection()
        current_app.logger.info("Database connections verified")
        
        # Get current year just like in process_single_school
        current_year = datetime.utcnow().year
        current_esri_year = current_year if datetime.utcnow().month >= 7 else current_year - 1
        current_app.logger.debug(f"Using current ESRI year: {current_esri_year}")
        
        if not esri_data:
            current_app.logger.error(f"No ESRI data provided for {ncessch}")
            return False
            
        # Check for existing data first like in process_single_school
        current_app.logger.debug(f"Checking for existing polygon relationships for {ncessch}")
        existing_polygons = SchoolPolygonRelationship.query.filter_by(ncessch=ncessch)\
            .filter_by(current_year=current_esri_year)\
            .first()
            
        if existing_polygons:
            current_app.logger.info(f"Found existing polygon relationships for {ncessch} from {current_esri_year}")
        else:
            current_app.logger.info(f"No existing polygon relationships found for {ncessch}")
        
        current_app.logger.debug(f"Looking up school record for {ncessch}")
        school = School.query.filter_by(ncessch=ncessch).first()
        if not school:
            current_app.logger.error(f"School not found for ncessch: {ncessch}")
            return False
        current_app.logger.debug(f"Found school: id={school.id}")
            
        current_app.logger.debug(f"Looking up directory entry for school {school.id}")
        directory_entry = DirectoryEntry.query\
            .filter_by(school_id=school.id)\
            .order_by(DirectoryEntry.data_year.desc())\
            .first()
            
        if not directory_entry:
            current_app.logger.error(f"No directory entry found for school {ncessch}")
            return False
        current_app.logger.debug(f"Found directory entry: lat={directory_entry.latitude}, lon={directory_entry.longitude}")

        # Get nearby schools data
        current_app.logger.debug(f"Getting nearby schools for current year ({current_esri_year})")
        current_year_schools = get_nearby_schools_for_year(2023, 
            directory_entry.latitude, 
            directory_entry.longitude)
        
        if not current_year_schools:
            current_app.logger.error(f"No current year schools found for {ncessch}")
            return False
        current_app.logger.debug(f"Found {len(current_year_schools)} current year schools")
        
        current_app.logger.debug(f"Getting nearby schools for comparison year (2019)")
        comparison_year_schools = get_nearby_schools_for_year(2019, 
            directory_entry.latitude, 
            directory_entry.longitude)
            
        if not comparison_year_schools:
            current_app.logger.error(f"No comparison year schools found for {ncessch}")
            return False
        current_app.logger.debug(f"Found {len(comparison_year_schools)} comparison year schools")

        relationships_updated = 0
        missing_polygons = []
        
        try:
            # Get engine with explicit bind
            current_app.logger.debug("Getting database engine")
            engine = get_db_engine('esri_data')
            current_app.logger.debug("Got database engine successfully")
            
            # Check data status like in process_single_school
            data_status = check_esri_data_status(engine, ncessch)
            current_app.logger.debug(f"ESRI data status for {ncessch}: {data_status}")
            
            current_app.logger.info(f"Starting transaction for {ncessch}")
            with engine.begin() as conn:
                current_app.logger.debug("Transaction started successfully")
                
                for drive_time, data in esri_data.items():
                    current_app.logger.debug(f"Processing drive time {drive_time}")
                    
                    if not data or 'drive_time_polygon' not in data or data['drive_time_polygon'] is None:
                        current_app.logger.warning(f"No polygon data for drive time {drive_time}")
                        missing_polygons.append(drive_time)
                        continue
                    
                    try:
                        # Parse polygon data
                        current_app.logger.debug(f"Parsing polygon data for drive time {drive_time}")
                        polygon_data = json.loads(data['drive_time_polygon'])
                        
                        if not polygon_data.get('rings'):
                            current_app.logger.error(f"No rings in polygon data for drive time {drive_time}")
                            raise ValueError("Invalid polygon data structure - missing rings")
                            
                        if not polygon_data['rings'][0]:
                            current_app.logger.error(f"Empty rings in polygon data for drive time {drive_time}")
                            raise ValueError("Invalid polygon data structure - empty rings")
                            
                        current_app.logger.debug(f"Creating polygon for drive time {drive_time}")
                        polygon = Polygon(polygon_data['rings'][0])
                        
                        if not polygon.is_valid:
                            current_app.logger.error(f"Invalid polygon geometry for drive time {drive_time}")
                            raise ValueError("Invalid polygon geometry")
                            
                        current_app.logger.debug(f"Successfully created polygon for drive time {drive_time}")
                        
                    except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
                        current_app.logger.error(f"Error processing polygon for drive time {drive_time}: {str(e)}")
                        missing_polygons.append(drive_time)
                        continue

                    # Find schools within polygon
                    current_app.logger.debug(f"Finding current year schools within polygon for drive time {drive_time}")
                    current_nearby = []
                    comparison_nearby = []
                    
                    for other_school, other_entry in current_year_schools.items():
                        if (other_entry.longitude is not None and 
                            other_entry.latitude is not None and 
                            polygon.contains(Point(other_entry.longitude, other_entry.latitude))):
                            current_nearby.append(other_school.ncessch)
                            
                    current_app.logger.debug(f"Finding comparison year schools within polygon for drive time {drive_time}")
                    for other_school, other_entry in comparison_year_schools.items():
                        if (other_entry.longitude is not None and 
                            other_entry.latitude is not None and 
                            polygon.contains(Point(other_entry.longitude, other_entry.latitude))):
                            comparison_nearby.append(other_school.ncessch)

                    current_app.logger.info(
                        f"Drive time {drive_time} minutes - Found {len(current_nearby)} schools within polygon for 2023"
                    )
                    current_app.logger.info(
                        f"Drive time {drive_time} minutes - Found {len(comparison_nearby)} schools within polygon for 2019"
                    )

                    try:
                        check_stmt = text("""
                            SELECT id FROM school_polygon_relationships 
                            WHERE ncessch = :ncessch 
                            AND drive_time = :drive_time
                            AND current_year = :current_year
                        """)

                        result = conn.execute(check_stmt, {
                            'ncessch': ncessch,
                            'drive_time': drive_time,
                            'current_year': 2023
                        }).first()

                        if result:
                            # Update existing record
                            current_app.logger.debug(f"Updating existing record {result.id}")
                            update_stmt = text("""
                                UPDATE school_polygon_relationships 
                                SET school_id = :school_id,
                                    current_nearby_schools = :current_nearby_schools,
                                    comparison_nearby_schools = :comparison_nearby_schools,
                                    comparison_year = :comparison_year
                                WHERE id = :id
                            """)
                            conn.execute(update_stmt, {
                                'id': result.id,
                                'school_id': school.id,
                                'current_nearby_schools': json.dumps(current_nearby),
                                'comparison_nearby_schools': json.dumps(comparison_nearby),
                                'comparison_year': 2019
                            })
                        else:
                            # Get a safe sequence value for new record
                            new_id = get_valid_sequence_id(conn)
                            current_app.logger.debug(f"Using validated sequence ID: {new_id} for new record")
                            
                            insert_stmt = text("""
                                INSERT INTO school_polygon_relationships (
                                    id, school_id, ncessch, drive_time,
                                    current_nearby_schools, comparison_nearby_schools,
                                    current_year, comparison_year
                                ) VALUES (
                                    :id, :school_id, :ncessch, :drive_time,
                                    :current_nearby_schools, :comparison_nearby_schools,
                                    :current_year, :comparison_year
                                )
                            """)
                            insert_params = {
                                'id': new_id,
                                'school_id': school.id,
                                'ncessch': ncessch,
                                'drive_time': drive_time,
                                'current_nearby_schools': json.dumps(current_nearby),
                                'comparison_nearby_schools': json.dumps(comparison_nearby),
                                'current_year': 2023,
                                'comparison_year': 2019
                            }
                            current_app.logger.debug(f"Insert params: {insert_params}")
                            conn.execute(insert_stmt, insert_params)

                        relationships_updated += 1
                        current_app.logger.debug(f"Successfully processed drive time {drive_time}")

                    except Exception as e:
                        current_app.logger.error(f"Database error for drive time {drive_time}: {str(e)}", exc_info=True)
                        raise

                if missing_polygons:
                    current_app.logger.error(f"Missing data for drive times: {set(missing_polygons)}")
                    
                if relationships_updated == 0:
                    current_app.logger.warning(f"No polygon relationships updated for {ncessch}")
                    return False

                # Verify the updates
                if relationships_updated > 0:
                    current_app.logger.info(f"About to verify {relationships_updated} entries for {ncessch}")
                    
                    try:
                        count = conn.execute(text(
                            "SELECT COUNT(*) FROM school_polygon_relationships WHERE ncessch = :ncessch"
                        ), {"ncessch": ncessch}).scalar()
                        
                        current_app.logger.debug(f"Verification found {count} entries for {ncessch}")
                        
                        if count != relationships_updated:
                            current_app.logger.error(
                                f"Data verification failed for {ncessch}: "
                                f"Expected {relationships_updated} entries but found {count}"
                            )
                            return False
                    except Exception as e:
                        current_app.logger.error(f"Error during verification query: {str(e)}", exc_info=True)
                        raise

                current_app.logger.info(f"Successfully updated {relationships_updated} polygon relationships for {ncessch}")
                return True
                
        except Exception as e:
            current_app.logger.error(f"Database transaction error for {ncessch}: {str(e)}", exc_info=True)
            raise
            
    except Exception as e:
        current_app.logger.error(f"Error updating polygon relationships for {ncessch}: {str(e)}", exc_info=True)
        return False
    finally:
        gc.collect()
    
def get_school_coordinates(ncessch):
    """Get coordinates for a school, handling both regular and split schools"""
    db = firestore.client()
    
    # Check if this is a split school
    parent_ncessch = ncessch.split('-')[0]
    split_doc = db.collection('school_splits').document(parent_ncessch).get()
    
    if split_doc.exists and '-' in ncessch:
        split_data = split_doc.to_dict()
        split_info = next(
            (s for s in split_data['splits'] if s['ncessch'] == ncessch),
            None
        )
        if split_info and 'address' in split_info:
            return (
                split_info['address'].get('latitude'),
                split_info['address'].get('longitude')
            )
    
    # Get coordinates from directory entry
    directory_entry = DirectoryEntry.query.join(School)\
        .filter(School.ncessch == ncessch)\
        .order_by(DirectoryEntry.data_year.desc())\
        .first()
        
    if directory_entry:
        if directory_entry.latitude and directory_entry.longitude:
            return directory_entry.latitude, directory_entry.longitude
        
        # If no coordinates but we have address, geocode it
        if directory_entry.street_address and directory_entry.city and directory_entry.state:
            try:
                geocoding_service = GeocodingService()
                coordinates = geocoding_service.geocode_address(
                    directory_entry.street_address,
                    directory_entry.city,
                    directory_entry.state,
                    directory_entry.zip_code
                )
                if coordinates:
                    # Update DirectoryEntry with new coordinates
                    directory_entry.latitude = coordinates.get('latitude')
                    directory_entry.longitude = coordinates.get('longitude')
                    db.session.commit()
                    return coordinates.get('latitude'), coordinates.get('longitude')
                
            except Exception as e:
                logging.error(f"Error geocoding address for {ncessch}: {str(e)}")
                
    return None, None

def check_esri_data_status(engine, ncessch):
   """Check if ESRI data needs updating based on current ESRI data year"""
   current_date = datetime.utcnow()
   current_esri_year = current_date.year if current_date.month >= 7 else current_date.year - 1
   esri_start = datetime(current_esri_year, 7, 1)
   esri_end = datetime(current_esri_year + 1, 7, 1)

   query = text("""
       SELECT id, timestamp, has_data 
       FROM esri_data 
       WHERE ncessch = :ncessch 
       AND timestamp >= :start_date
       AND timestamp < :end_date
       AND has_data = 1
   """)
   
   with engine.connect() as conn:
       result = conn.execute(query, {
           "ncessch": ncessch,
           "start_date": esri_start,
           "end_date": esri_end
       }).fetchall()
       return "current" if result else "needs_update"
    

def process_single_school(ncessch):
    """Process ESRI data for a single school, checking existing data first"""
    try:
        # Ensure sequences are aligned first
        ensure_sequence_alignment()
        current_app.logger.info(f"Starting to process school {ncessch}")
        
        # Run database verification first
        verify_nces_connection()
        verify_esri_connection()
        
        current_app.logger.info(f"Checking existing ESRI data for school {ncessch}")
        
        # Get current year
        current_year = datetime.utcnow().year
        current_esri_year = current_year if datetime.utcnow().month >= 7 else current_year - 1
        
        # First check for existing data
        existing_data = EsriData.query.filter_by(ncessch=ncessch)\
            .filter(EsriData.timestamp >= datetime(current_year - 1, 7, 1))\
            .filter(EsriData.timestamp <= datetime.utcnow())\
            .filter(EsriData.has_data == 1)\
            .first()
        
        # Check for existing polygon relationships
        existing_polygons = SchoolPolygonRelationship.query.filter_by(ncessch=ncessch)\
            .filter_by(current_year=current_esri_year)\
            .first()
        
        needs_esri_data = not existing_data
        needs_polygons = not existing_polygons

        if not needs_esri_data and not needs_polygons:
            current_app.logger.info(f"Using existing ESRI data and polygons for school {ncessch}")
            return True
        
        # Log what we're going to do
        if existing_data:
            current_app.logger.info(f"Using existing ESRI data for school {ncessch} from {current_esri_year}")
        
        if needs_esri_data:
            # Get coordinates
            current_app.logger.debug(f"Getting coordinates for {ncessch}")
            latitude, longitude = get_school_coordinates(ncessch)
            if not latitude or not longitude:
                current_app.logger.warning(f"No coordinates found for {ncessch}")
                return False

            # Check data status before making API call
            engine = get_db_engine('esri_data')
            data_status = check_esri_data_status(engine, ncessch)
            
            if data_status != "current":
                current_app.logger.debug(f"Fetching ESRI data for {ncessch}")
                esri_data = fetch_esri_data(latitude, longitude)

                # Use a transaction to ensure atomicity
                with engine.begin() as conn:
                    stored_entries = 0
                    current_app.logger.info(f"Starting transaction for {ncessch}")
                    for drive_time, data in esri_data.items():
                        try:
                            # Prepare the fields for update/insert
                            esri_fields = {k: v for k, v in data.items() 
                                        if hasattr(EsriData, k) and k != 'id'}
                            
                            esri_fields.update({
                                'ncessch': ncessch,
                                'latitude': latitude,
                                'longitude': longitude,
                                'drive_time': drive_time,
                                'timestamp': datetime.utcnow(),
                                'has_data': 1
                            })
                            
                            # First check if record exists
                            check_stmt = text("""
                                SELECT id FROM esri_data 
                                WHERE ncessch = :ncessch 
                                AND drive_time = :drive_time
                                AND EXTRACT(year FROM timestamp) = :current_esri_year
                            UNION
                                SELECT id FROM esri_data 
                                WHERE id = (SELECT nextval('esri_data_id_seq'))
                            """)

                            result = conn.execute(check_stmt, {
                                'ncessch': ncessch,
                                'drive_time': drive_time,
                                'current_esri_year': current_esri_year
                            }).first()

                            if result:
                                # Update existing record
                                update_stmt = text("""
                                    UPDATE esri_data 
                                    SET latitude = :latitude,
                                        longitude = :longitude,
                                        source_country = :source_country,
                                        area_type = :area_type,
                                        buffer_units = :buffer_units,
                                        buffer_units_alias = :buffer_units_alias,
                                        buffer_radii = :buffer_radii,
                                        aggregation_method = :aggregation_method,
                                        population_to_polygon_size_rating = :population_to_polygon_size_rating,
                                        apportionment_confidence = :apportionment_confidence,
                                        has_data = :has_data,
                                        drive_time_polygon = :drive_time_polygon,
                                        timestamp = :timestamp,
                                        age4_cy = :age4_cy,
                                        age5_cy = :age5_cy,
                                        age6_cy = :age6_cy,
                                        age7_cy = :age7_cy,
                                        age8_cy = :age8_cy,
                                        age9_cy = :age9_cy,
                                        age10_cy = :age10_cy,
                                        age11_cy = :age11_cy,
                                        age12_cy = :age12_cy,
                                        age13_cy = :age13_cy,
                                        age14_cy = :age14_cy,
                                        age15_cy = :age15_cy,
                                        age16_cy = :age16_cy,
                                        age17_cy = :age17_cy,
                                        age4_fy = :age4_fy,
                                        age5_fy = :age5_fy,
                                        age6_fy = :age6_fy,
                                        age7_fy = :age7_fy,
                                        age8_fy = :age8_fy,
                                        age9_fy = :age9_fy,
                                        age10_fy = :age10_fy,
                                        age11_fy = :age11_fy,
                                        age12_fy = :age12_fy,
                                        age13_fy = :age13_fy,
                                        age14_fy = :age14_fy,
                                        age15_fy = :age15_fy,
                                        age16_fy = :age16_fy,
                                        age17_fy = :age17_fy,
                                        age4_c20 = :age4_c20,
                                        age5_c20 = :age5_c20,
                                        age6_c20 = :age6_c20,
                                        age7_c20 = :age7_c20,
                                        age8_c20 = :age8_c20,
                                        age9_c20 = :age9_c20,
                                        age10_c20 = :age10_c20,
                                        age11_c20 = :age11_c20,
                                        age12_c20 = :age12_c20,
                                        age13_c20 = :age13_c20,
                                        age14_c20 = :age14_c20,
                                        age15_c20 = :age15_c20,
                                        age16_c20 = :age16_c20,
                                        age17_c20 = :age17_c20,
                                        nhadltwh20 = :nhadltwh20,
                                        nhadltbl20 = :nhadltbl20,
                                        nhadltas20 = :nhadltas20,
                                        nhadltpi20 = :nhadltpi20,
                                        nhadltai20 = :nhadltai20,
                                        nhadltot20 = :nhadltot20,
                                        nhadlt2_r20 = :nhadlt2_r20,
                                        hadults20 = :hadults20,
                                        nhwu18_c20 = :nhwu18_c20,
                                        nhbu18_c20 = :nhbu18_c20,
                                        nhasu18_c20 = :nhasu18_c20,
                                        nhpiu18_c20 = :nhpiu18_c20,
                                        nhaiu18_c20 = :nhaiu18_c20,
                                        nhou18_c20 = :nhou18_c20,
                                        nhmu18_c20 = :nhmu18_c20,
                                        hu18_rbs20 = :hu18_rbs20,
                                        per_hisp_adult_20 = :per_hisp_adult_20,
                                        per_wht_adult_20 = :per_wht_adult_20,
                                        per_blk_adult_20 = :per_blk_adult_20,
                                        per_asn_adult_20 = :per_asn_adult_20,
                                        per_pi_adult_20 = :per_pi_adult_20,
                                        per_ai_adult_20 = :per_ai_adult_20,
                                        per_other_adult_20 = :per_other_adult_20,
                                        per_two_or_more_adult_20 = :per_two_or_more_adult_20,
                                        per_hisp_child_20 = :per_hisp_child_20,
                                        per_wht_child_20 = :per_wht_child_20,
                                        per_blk_child_20 = :per_blk_child_20,
                                        per_asn_child_20 = :per_asn_child_20,
                                        per_pi_child_20 = :per_pi_child_20,
                                        per_ai_child_20 = :per_ai_child_20,
                                        per_other_child_20 = :per_other_child_20,
                                        per_two_or_more_child_20 = :per_two_or_more_child_20,
                                        medhinc_cy = :medhinc_cy,
                                        hincbasecy = :hincbasecy,
                                        hinc0_cy = :hinc0_cy,
                                        hinc15_cy = :hinc15_cy,
                                        hinc25_cy = :hinc25_cy,
                                        hinc35_cy = :hinc35_cy,
                                        tothu_cy = :tothu_cy,
                                        renter_cy = :renter_cy,
                                        vacant_cy = :vacant_cy,
                                        per_50k_cy = :per_50k_cy,
                                        per_renter_cy = :per_renter_cy,
                                        per_vacant_cy = :per_vacant_cy
                                    WHERE id = :id
                                """)
                                conn.execute(update_stmt, {**esri_fields, 'id': result.id})
                            else:
                                # Insert new record
                                insert_stmt = text("""
                                    INSERT INTO esri_data (
                                        ncessch, latitude, longitude, drive_time,
                                        source_country, area_type, buffer_units, buffer_units_alias,
                                        buffer_radii, aggregation_method, population_to_polygon_size_rating,
                                        apportionment_confidence, has_data, drive_time_polygon, timestamp,
                                        age4_cy, age5_cy, age6_cy, age7_cy, age8_cy, age9_cy, age10_cy,
                                        age11_cy, age12_cy, age13_cy, age14_cy, age15_cy, age16_cy, age17_cy,
                                        age4_fy, age5_fy, age6_fy, age7_fy, age8_fy, age9_fy, age10_fy,
                                        age11_fy, age12_fy, age13_fy, age14_fy, age15_fy, age16_fy, age17_fy,
                                        age4_c20, age5_c20, age6_c20, age7_c20, age8_c20, age9_c20, age10_c20,
                                        age11_c20, age12_c20, age13_c20, age14_c20, age15_c20, age16_c20, age17_c20,
                                        nhadltwh20, nhadltbl20, nhadltas20, nhadltpi20, nhadltai20, nhadltot20,
                                        nhadlt2_r20, hadults20, nhwu18_c20, nhbu18_c20, nhasu18_c20, nhpiu18_c20,
                                        nhaiu18_c20, nhou18_c20, nhmu18_c20, hu18_rbs20, per_hisp_adult_20,
                                        per_wht_adult_20, per_blk_adult_20, per_asn_adult_20, per_pi_adult_20,
                                        per_ai_adult_20, per_other_adult_20, per_two_or_more_adult_20,
                                        per_hisp_child_20, per_wht_child_20, per_blk_child_20, per_asn_child_20,
                                        per_pi_child_20, per_ai_child_20, per_other_child_20, per_two_or_more_child_20,
                                        medhinc_cy, hincbasecy, hinc0_cy, hinc15_cy, hinc25_cy, hinc35_cy,
                                        tothu_cy, renter_cy, vacant_cy, per_50k_cy, per_renter_cy, per_vacant_cy
                                    ) VALUES (
                                        :ncessch, :latitude, :longitude, :drive_time,
                                        :source_country, :area_type, :buffer_units, :buffer_units_alias,
                                        :buffer_radii, :aggregation_method, :population_to_polygon_size_rating,
                                        :apportionment_confidence, :has_data, :drive_time_polygon, :timestamp,
                                        :age4_cy, :age5_cy, :age6_cy, :age7_cy, :age8_cy, :age9_cy, :age10_cy,
                                        :age11_cy, :age12_cy, :age13_cy, :age14_cy, :age15_cy, :age16_cy, :age17_cy,
                                        :age4_fy, :age5_fy, :age6_fy, :age7_fy, :age8_fy, :age9_fy, :age10_fy,
                                        :age11_fy, :age12_fy, :age13_fy, :age14_fy, :age15_fy, :age16_fy, :age17_fy,
                                        :age4_c20, :age5_c20, :age6_c20, :age7_c20, :age8_c20, :age9_c20, :age10_c20,
                                        :age11_c20, :age12_c20, :age13_c20, :age14_c20, :age15_c20, :age16_c20, :age17_c20,
                                        :nhadltwh20, :nhadltbl20, :nhadltas20, :nhadltpi20, :nhadltai20, :nhadltot20,
                                        :nhadlt2_r20, :hadults20, :nhwu18_c20, :nhbu18_c20, :nhasu18_c20, :nhpiu18_c20,
                                        :nhaiu18_c20, :nhou18_c20, :nhmu18_c20, :hu18_rbs20, :per_hisp_adult_20,
                                        :per_wht_adult_20, :per_blk_adult_20, :per_asn_adult_20, :per_pi_adult_20,
                                        :per_ai_adult_20, :per_other_adult_20, :per_two_or_more_adult_20,
                                        :per_hisp_child_20, :per_wht_child_20, :per_blk_child_20, :per_asn_child_20,
                                        :per_pi_child_20, :per_ai_child_20, :per_other_child_20, :per_two_or_more_child_20,
                                        :medhinc_cy, :hincbasecy, :hinc0_cy, :hinc15_cy, :hinc25_cy, :hinc35_cy,
                                        :tothu_cy, :renter_cy, :vacant_cy, :per_50k_cy, :per_renter_cy, :per_vacant_cy
                                    )
                                """)
                                conn.execute(insert_stmt, esri_fields)
                            
                            current_app.logger.info(f"Successfully inserted data for drive time {drive_time}")
                            stored_entries += 1
                        except Exception as e:
                            current_app.logger.error(f"Error processing ESRI data for drive time {drive_time}: {str(e)}")
                            raise

                    if stored_entries > 0:
                        current_app.logger.info(f"About to verify {stored_entries} entries for {ncessch}")
                        # Count verification
                        count = conn.execute(text(
                            "SELECT COUNT(*) FROM esri_data WHERE ncessch = :ncessch"
                        ), {"ncessch": ncessch}).scalar()
                        
                        if count != stored_entries:
                            current_app.logger.error(
                                f"Data verification failed for {ncessch}: "
                                f"Expected {stored_entries} entries but found {count}"
                            )
                            return False

                        # Drive times verification
                        drive_times_result = conn.execute(text(
                            "SELECT array_agg(drive_time) FROM esri_data WHERE ncessch = :ncessch"
                        ), {"ncessch": ncessch}).scalar()
                        
                        expected_drive_times = set(esri_data.keys())
                        stored_drive_times = set(drive_times_result) if drive_times_result else set()
                        
                        if expected_drive_times != stored_drive_times:
                            missing_times = expected_drive_times - stored_drive_times
                            current_app.logger.error(
                                f"Missing drive times for {ncessch}: {missing_times}"
                            )
                            return False

                        current_app.logger.info(
                            f"Successfully verified {count} ESRI entries for {ncessch} "
                            f"with drive times: {sorted(stored_drive_times)}"
                        )
            else:
                current_app.logger.info(f"Using existing ESRI data for {ncessch}")
                needs_esri_data = False

        # Always process polygons if needed, regardless of ESRI data source
        if needs_polygons:
            current_app.logger.info(f"Processing polygon relationships for school {ncessch}")
            esri_data_for_polygons = {}
            expected_drive_times = {5, 10, 15}  # The three drive times we expect
            
            if needs_esri_data:
                current_app.logger.debug("Using newly fetched ESRI data for polygons")
                for drive_time, data in esri_data.items():
                    if data.get('drive_time_polygon'):
                        esri_data_for_polygons[drive_time] = {
                            'drive_time_polygon': data['drive_time_polygon']
                        }
                        current_app.logger.debug(f"Added polygon for drive time {drive_time}")
                    else:
                        current_app.logger.warning(f"Missing polygon data for drive time {drive_time}")
            else:
                # Fetch existing ESRI data for polygon processing
                current_app.logger.debug("Fetching existing ESRI data for polygons")
                records = EsriData.query.filter_by(ncessch=ncessch)\
                    .filter(EsriData.timestamp >= datetime(current_year - 1, 7, 1))\
                    .filter(EsriData.timestamp <= datetime.utcnow())\
                    .filter(EsriData.has_data == 1)\
                    .all()
                
                current_app.logger.debug(f"Found {len(records)} existing ESRI records")
                found_drive_times = set()
                
                for record in records:
                    found_drive_times.add(record.drive_time)
                    if record.drive_time_polygon:
                        esri_data_for_polygons[record.drive_time] = {
                            'drive_time_polygon': record.drive_time_polygon
                        }
                        current_app.logger.debug(f"Added polygon for drive time {record.drive_time}")
                    else:
                        current_app.logger.warning(f"No polygon found for drive time {record.drive_time}")
                
                # Check for missing drive times
                missing_drive_times = expected_drive_times - found_drive_times
                if missing_drive_times:
                    current_app.logger.error(f"Missing data for drive times: {missing_drive_times}")
            
            if not esri_data_for_polygons:
                current_app.logger.error(f"No valid polygon data found for any drive time")
                return False
            
            found_times = set(esri_data_for_polygons.keys())
            if found_times != expected_drive_times:
                current_app.logger.error(f"Missing polygons for drive times. Expected {expected_drive_times}, got {found_times}")
                return False
                
            current_app.logger.info(f"Found all required polygons: {sorted(found_times)}")
            
            # Update polygon relationships
            polygon_success = update_polygon_relationships(ncessch, esri_data_for_polygons)
            if not polygon_success:
                current_app.logger.error(f"Failed to update polygon relationships for {ncessch}")
                return False

        return True
            
    except Exception as e:
        current_app.logger.error(f"Error processing school {ncessch}: {str(e)}")
        return False

    
def delete_esri_data(ncessch):
    """Delete ESRI data and polygon relationships for a school"""
    try:
        current_app.logger.info(f"Starting ESRI data deletion for {ncessch}")
        
        # Get ESRI database engine
        engine = get_db_engine('esri_data')
        
        with engine.begin() as conn:
            # Delete from esri_data table
            delete_esri = text("""
                DELETE FROM esri_data 
                WHERE ncessch = :ncessch
            """)
            esri_result = conn.execute(delete_esri, {'ncessch': ncessch})
            current_app.logger.info(f"Deleted {esri_result.rowcount} records from esri_data for {ncessch}")
            
            # Delete from school_polygon_relationships table
            delete_polygons = text("""
                DELETE FROM school_polygon_relationships 
                WHERE ncessch = :ncessch
            """)
            polygon_result = conn.execute(delete_polygons, {'ncessch': ncessch})
            current_app.logger.info(f"Deleted {polygon_result.rowcount} records from school_polygon_relationships for {ncessch}")
            
            return True
            
    except Exception as e:
        current_app.logger.error(f"Error deleting ESRI data for {ncessch}: {str(e)}", exc_info=True)
        return False

def get_school_status(ncessch):
    """Get current status for a school"""
    db = get_esri_db()
    query = text("SELECT * FROM school_processing_status WHERE ncessch = :ncessch")
    
    with db.connect() as conn:
        result = conn.execute(query, {'ncessch': ncessch}).fetchone()
    return result

def cleanup_old_statuses():
    """Optional: Clean up old status entries"""
    db = get_esri_db()
    query = text("DELETE FROM school_processing_status WHERE timestamp < NOW() - INTERVAL 24 HOUR")
    
    with db.connect() as conn:
        conn.execute(query)
        conn.commit()