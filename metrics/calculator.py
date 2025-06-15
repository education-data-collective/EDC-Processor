from datetime import datetime
from flask import current_app
from app.models import School, MembershipData, DirectoryEntry, EsriData, DistrictMetrics
from enrollment_projections.main import generate_and_update_projections
from .utils import (
    calculate_grade_filtered_population,
    calculate_enrollment,
    get_school_grades,
    calculate_market_share,
    calculate_percent_change,
    get_status,
    check_newer_school,
    validate_ncessch,
    POPULATION_THRESHOLD,
    PROJECTION_THRESHOLD,
    MARKET_SHARE_THRESHOLD,
    ENROLLMENT_THRESHOLD
)
from sqlalchemy.dialects.postgresql import insert
import logging

logger = logging.getLogger(__name__)

async def calculate_district_metrics(nces_session, esri_session, firestore_db, schools):
    """Calculate and store district metrics for given schools"""
    try:
        total_schools = len(schools)
        logger.info(f"Starting district metrics calculation for {total_schools} schools")
        
        success_count = 0
        error_count = 0
        batch_size = 100
        current_batch = []
        
        data_versions = {
            'membership_data_year': 2023,
            'comparison_year': 2019,
            'esri_data_year': 2023,
            'processed_at': datetime.utcnow().isoformat()
        }
        
        # Get school IDs for query filtering
        school_ids = [school.id for school in schools]
        
        # Query enrollment data using NCES session
        enrollment_data = nces_session.query(MembershipData).\
            filter(MembershipData.school_id.in_(school_ids)).\
            filter(MembershipData.data_year.in_([2019, 2023])).\
            all()
            
        # Create enrollment lookup
        enrollment_lookup = {
            school.ncessch: {
                'current': {entry.grade: entry.total_membership 
                    for entry in enrollment_data 
                    if entry.school_id == school.id and entry.data_year == 2023},
                'comparison': {entry.grade: entry.total_membership 
                    for entry in enrollment_data 
                    if entry.school_id == school.id and entry.data_year == 2019}
            }
            for school in schools
        }
        
        # Query and process ESRI data
        esri_data = esri_session.query(EsriData).\
            filter(EsriData.ncessch.in_([school.ncessch for school in schools])).\
            filter(EsriData.drive_time == 10).\
            all()
            
        esri_lookup = {
            data.ncessch: {
                'ages': {
                    '4_17': {
                        'current': [getattr(data, f'age{i}_cy') for i in range(4, 18)],
                        'future': [getattr(data, f'age{i}_fy') for i in range(4, 18)],
                        '2020': [getattr(data, f'age{i}_c20') for i in range(4, 18)]
                    }
                }
            }
            for data in esri_data
        }

        # Process each school
        for i, school in enumerate(schools, 1):
            try:
                # Validate NCESSCH before processing
                try:
                    validated_ncessch = validate_ncessch(school.ncessch)
                except ValueError as ve:
                    logger.error(f"Invalid NCESSCH for school {school.ncessch}: {str(ve)}")
                    error_count += 1
                    continue

                logger.info(f"Processing metrics for school {validated_ncessch} ({i}/{total_schools})")
                
                # Get data from lookups
                school_enrollment = enrollment_lookup.get(validated_ncessch, {})
                school_esri = esri_lookup.get(validated_ncessch, {})
                
                # Get projections following the projections route pattern
                projections = await get_school_projections(
                    validated_ncessch, 
                    firestore_db, 
                    user_role='admin'  # Pass admin role to check both projection types
                )
                
                # Get current grades and calculate metrics
                current_grades = get_school_grades({'enrollment_by_grade': school_enrollment})
                
                if not current_grades:
                    logger.warning(f"No grades found for school {validated_ncessch}")
                    error_count += 1
                    continue
                
                # Calculate population metrics
                pop_totals = calculate_grade_filtered_population(school_esri, current_grades)
                pop_trends = {
                    'past_to_latest': calculate_percent_change(pop_totals['current'], pop_totals['past']),
                    'latest_to_projected': calculate_percent_change(pop_totals['future'], pop_totals['current'])
                }
                
                # Calculate enrollment metrics
                enrollments = {
                    'current': calculate_enrollment(school_enrollment.get('current', {}), current_grades),
                    'past': calculate_enrollment(school_enrollment.get('comparison', {}), current_grades)
                }
                
                # Calculate market shares
                market_shares = {
                    'current': calculate_market_share(enrollments['current'], pop_totals['current']),
                    'past': calculate_market_share(enrollments['past'], pop_totals['past'])
                }
                
                # Calculate trends
                enrollment_trend_past_to_latest = calculate_percent_change(
                    enrollments['current'], 
                    enrollments['past']
                )
                
                # Calculate projected enrollment trend
                projected_enrollment = (projections['updated_projected'] 
                                     if projections['projection_type'] == 'updated' 
                                     else projections['public_projected'])
                
                enrollment_trend_latest_to_projected = calculate_percent_change(
                    projected_enrollment,
                    enrollments['current']
                )

                # Create metrics record within a savepoint
                with esri_session.begin_nested():
                    metrics = DistrictMetrics(
                        ncessch=validated_ncessch,
                        calculated_at=datetime.utcnow(),
                        data_versions=data_versions,
                        population_past=int(pop_totals['past']),
                        population_current=int(pop_totals['current']),
                        population_future=int(pop_totals['future']),
                        population_trend_past_to_latest=min(max(round(pop_trends['past_to_latest'], 2), -999.99), 999.99),
                        population_trend_latest_to_projected=min(max(round(pop_trends['latest_to_projected'], 2), -999.99), 999.99),
                        population_trend_status=get_status(pop_trends['past_to_latest'], POPULATION_THRESHOLD),
                        population_projection_status=get_status(pop_trends['latest_to_projected'], PROJECTION_THRESHOLD),
                        market_share_past=min(round(market_shares['past'], 2), 999.99),
                        market_share_current=min(round(market_shares['current'], 2), 999.99),
                        market_share_trend=min(max(round(market_shares['current'] - market_shares['past'], 2), -999.99), 999.99),
                        market_share_status=get_status(
                            market_shares['current'] - market_shares['past'], 
                            MARKET_SHARE_THRESHOLD, 
                            'market_share'
                        ),
                        enrollment_past=int(enrollments['past']),
                        enrollment_current=int(enrollments['current']),
                        public_enrollment_projected=int(projections['public_projected']),
                        updated_enrollment_projected=int(projections['updated_projected']),
                        projection_type=projections['projection_type'],
                        enrollment_trend_past_to_latest=min(max(round(enrollment_trend_past_to_latest, 2), -999.99), 999.99),
                        enrollment_trend_latest_to_projected=min(max(round(enrollment_trend_latest_to_projected, 2), -999.99), 999.99),
                        enrollment_trend_status=get_status(enrollment_trend_past_to_latest, ENROLLMENT_THRESHOLD),
                        enrollment_projection_status=get_status(enrollment_trend_latest_to_projected, PROJECTION_THRESHOLD),
                        is_newer=check_newer_school(school_enrollment),
                        has_projections=projections['has_projections']
                    )
                    
                    # Just append to batch, don't count success yet
                    current_batch.append(metrics)
                
                # Commit batch if we've reached batch_size
                if len(current_batch) >= batch_size:
                    try:
                        for metrics in current_batch:
                            if await batch_insert_metrics(esri_session, metrics.__dict__):
                                success_count += 1
                            else:
                                error_count += 1
                        logger.info(f"Committed batch of {len(current_batch)} schools")
                        current_batch = []
                    except Exception as e:
                        logger.error(f"Error committing batch: {str(e)}")
                        esri_session.rollback()
                        error_count += len(current_batch)
                        current_batch = []
                
                if i % 100 == 0:  # Log progress every 100 schools
                    logger.info(f"Processed {i}/{total_schools} schools. Success: {success_count}, Errors: {error_count}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing school {school.ncessch}: {str(e)}")
                continue
        
        # Commit any remaining schools
        if current_batch:
            try:
                for metrics in current_batch:
                    if await batch_insert_metrics(esri_session, metrics.__dict__):
                        success_count += 1
                    else:
                        error_count += 1
                logger.info(f"Committed final batch of {len(current_batch)} schools")
            except Exception as e:
                logger.error(f"Error committing final batch: {str(e)}")
                esri_session.rollback()
                error_count += len(current_batch)
                
        # Log final statistics
        logger.info(f"Processing complete. Total: {total_schools}, Success: {success_count}, Errors: {error_count}")
        return success_count > 0  # Only return true if at least one school succeeded
        
    except Exception as e:
        logger.error(f"Error calculating district metrics: {str(e)}")
        esri_session.rollback()
        return False
    
async def batch_insert_metrics(session, metrics_data):
    """Insert or update metrics data in batches using upsert"""
    try:
        # Clean the data by removing SQLAlchemy state
        clean_data = {}
        for key, value in metrics_data.items():
            # Skip SQLAlchemy internal attributes
            if not key.startswith('_sa_'):
                clean_data[key] = value
        
        # Create upsert statement
        stmt = insert(DistrictMetrics).values(clean_data)
        
        # Add ON CONFLICT DO UPDATE clause
        stmt = stmt.on_conflict_do_update(
            index_elements=['ncessch'],  # The primary key column
            set_={
                'calculated_at': stmt.excluded.calculated_at,
                'data_versions': stmt.excluded.data_versions,
                'population_past': stmt.excluded.population_past,
                'population_current': stmt.excluded.population_current,
                'population_future': stmt.excluded.population_future,
                'population_trend_past_to_latest': stmt.excluded.population_trend_past_to_latest,
                'population_trend_latest_to_projected': stmt.excluded.population_trend_latest_to_projected,
                'population_trend_status': stmt.excluded.population_trend_status,
                'population_projection_status': stmt.excluded.population_projection_status,
                'market_share_past': stmt.excluded.market_share_past,
                'market_share_current': stmt.excluded.market_share_current,
                'market_share_trend': stmt.excluded.market_share_trend,
                'market_share_status': stmt.excluded.market_share_status,
                'enrollment_past': stmt.excluded.enrollment_past,
                'enrollment_current': stmt.excluded.enrollment_current,
                'public_enrollment_projected': stmt.excluded.public_enrollment_projected,
                'updated_enrollment_projected': stmt.excluded.updated_enrollment_projected,
                'projection_type': stmt.excluded.projection_type,
                'enrollment_trend_past_to_latest': stmt.excluded.enrollment_trend_past_to_latest,
                'enrollment_trend_latest_to_projected': stmt.excluded.enrollment_trend_latest_to_projected,
                'enrollment_trend_status': stmt.excluded.enrollment_trend_status,
                'enrollment_projection_status': stmt.excluded.enrollment_projection_status,
                'is_newer': stmt.excluded.is_newer,
                'has_projections': stmt.excluded.has_projections
            }
        )
        
        # Execute the upsert
        result = session.execute(stmt)
        session.commit()
        return True
        
    except Exception as e:
        current_app.logger.error(f"Error in batch_insert_metrics: {str(e)}")
        session.rollback()
        return False
    
async def process_metrics_batch(session, metrics_batch):
    """Process a batch of metrics records"""
    success = 0
    errors = 0
    
    for metrics in metrics_batch:
        try:
            if await batch_insert_metrics(session, metrics.__dict__):
                success += 1
            else:
                errors += 1
        except Exception as e:
            logger.error(f"Error processing metric: {str(e)}")
            errors += 1
            
    return {
        'success': success,
        'errors': errors
    }

async def get_school_projections(ncessch, firestore_db, user_role=None):
    """Get school projections following the same logic as the projections route"""
    school_ref = firestore_db.collection('schools').document(ncessch)
    projection_data = {
        'has_projections': False,
        'projection_type': 'none',
        'public_projected': 0,
        'updated_projected': 0
    }
    
    try:
        # Try updated projections first if user has proper role
        if user_role in ['admin', 'network_leader']:
            logger.info(f"Checking updated projections for {ncessch}")
            proj_ref = school_ref.collection('updated_projections').document('current')
            proj_doc = proj_ref.get()
            
            if proj_doc.exists:
                logger.info(f"Found existing updated projections for {ncessch}")
                proj_data = proj_doc.to_dict()
                logger.debug(f"Updated projection data structure: {proj_data.keys() if proj_data else None}")
                
                # Get updated projected enrollment
                if proj_data and 'projections' in proj_data:
                    logger.debug(f"Found projections key in updated data: {proj_data['projections'].keys()}")
                    logger.debug(f"Median projections: {proj_data['projections'].get('median', {}).keys()}")
                    
                    # Find latest year and get total enrollment
                    median_projections = proj_data['projections'].get('median', {})
                    if median_projections:
                        latest_year = sorted(median_projections.keys())[-1]
                        total = sum(median_projections[latest_year].values())
                        logger.debug(f"Latest year: {latest_year}, Total: {total}")
                        
                        projection_data.update({
                            'has_projections': True,
                            'projection_type': 'updated',
                            'updated_projected': total
                        })
                        return projection_data
            
            # Check enrollment data
            logger.info(f"Checking enrollment data for {ncessch}")
            enrollment_ref = school_ref.collection('enrollment_data').document('current')
            enrollment_doc = enrollment_ref.get()
            
            if enrollment_doc.exists:
                logger.info(f"Found enrollment data for {ncessch}, generating updated projections")
                projections = generate_and_update_projections(ncessch, enrollment_doc.to_dict())
                logger.debug(f"Generated projections structure: {projections.keys() if projections else None}")
                
                if projections and 'projections' in projections:
                    # Save the new projections
                    proj_ref.set(projections)
                    
                    # Get total from median projections
                    median_projections = projections['projections'].get('median', {})
                    if median_projections:
                        latest_year = sorted(median_projections.keys())[-1]
                        total = sum(median_projections[latest_year].values())
                        logger.debug(f"Generated projections - Latest year: {latest_year}, Total: {total}")
                        
                        projection_data.update({
                            'has_projections': True,
                            'projection_type': 'updated',
                            'updated_projected': total
                        })
                        return projection_data

        # Check public projections
        logger.info(f"Checking public projections for {ncessch}")
        proj_ref = school_ref.collection('public_projections').document('current')
        proj_doc = proj_ref.get()
        
        if proj_doc.exists:
            logger.info(f"Found existing public projections for {ncessch}")
            proj_data = proj_doc.to_dict()
            logger.debug(f"Public projection data structure: {proj_data.keys() if proj_data else None}")
            
            if proj_data and 'projections' in proj_data:
                logger.debug(f"Found projections key in public data: {proj_data['projections'].keys()}")
                logger.debug(f"Median projections: {proj_data['projections'].get('median', {}).keys()}")
                
                # Get total from median projections
                median_projections = proj_data['projections'].get('median', {})
                if median_projections:
                    latest_year = sorted(median_projections.keys())[-1]
                    total = sum(median_projections[latest_year].values())
                    logger.debug(f"Latest year: {latest_year}, Total: {total}")
                    
                    projection_data.update({
                        'has_projections': True,
                        'projection_type': 'public',
                        'public_projected': total
                    })
                    logger.debug(f"Updated projection data: {projection_data}")
                    return projection_data
        
        # Generate new public projections if none found
        logger.info(f"Generating new public projections for {ncessch}")
        projections = generate_and_update_projections(ncessch, None)
        logger.debug(f"Generated public projections structure: {projections.keys() if projections else None}")
        
        if projections and 'projections' in projections:
            # Save to Firestore
            proj_ref.set(projections)
            
            # Get total from median projections
            median_projections = projections['projections'].get('median', {})
            if median_projections:
                latest_year = sorted(median_projections.keys())[-1]
                total = sum(median_projections[latest_year].values())
                logger.debug(f"Generated public projections - Latest year: {latest_year}, Total: {total}")
                
                projection_data.update({
                    'has_projections': True,
                    'projection_type': 'public',
                    'public_projected': total
                })
                logger.debug(f"Final projection data: {projection_data}")
                return projection_data
            
        logger.warning(f"Failed to find or generate valid projections for {ncessch}")
            
    except Exception as e:
        logger.error(f"Error processing projections for {ncessch}: {str(e)}", exc_info=True)
    
    logger.warning(f"No valid projections found or generated for {ncessch}")
    logger.debug(f"Returning default projection data: {projection_data}")
    return projection_data