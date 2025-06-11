"""
Entity Processing Routes

Main API endpoints for processing schools and location points.
"""

from flask import request, jsonify, current_app
from .. import entity_bp
from ..processor import EntityProcessor
from ..utils import validate_entity, success_response, error_response


@entity_bp.route('/process', methods=['POST'])
async def process_entity():
    """Process a single entity (school or location point)"""
    try:
        data = request.get_json()
        entity_id = data.get('entity_id')
        entity_type = data.get('entity_type', 'school')
        data_year = data.get('data_year')
        
        current_app.logger.info(f"Processing {entity_type} {entity_id}")
        
        if not entity_id:
            return jsonify(error_response("entity_id is required")), 400
        
        # Validate entity exists and has required data
        validation_result = validate_entity(entity_id, entity_type)
        if not validation_result['valid']:
            return jsonify(error_response(validation_result['error'])), 400
        
        # Create processor and start processing
        processor = EntityProcessor(entity_id, entity_type, data_year)
        success, error_msg = await processor.process()
        
        if success:
            return jsonify(success_response({
                'entity_id': entity_id,
                'entity_type': entity_type,
                'stages_processed': processor.stages
            }, "Processing completed successfully"))
        else:
            return jsonify(error_response(error_msg)), 500
            
    except Exception as e:
        current_app.logger.error(f"Processing error: {str(e)}", exc_info=True)
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/process/bulk', methods=['POST'])
async def bulk_process():
    """Process multiple entities"""
    try:
        data = request.get_json()
        entities = data.get('entities', [])
        entity_type = data.get('entity_type', 'school')
        data_year = data.get('data_year')
        
        current_app.logger.info(f"Bulk processing {len(entities)} {entity_type}s")
        
        if not entities:
            return jsonify(error_response("entities list is required")), 400
        
        results = {
            'processed': [],
            'failed': [],
            'skipped': []
        }
        
        for entity_data in entities:
            entity_id = entity_data.get('entity_id') if isinstance(entity_data, dict) else entity_data
            
            try:
                # Validate entity
                validation_result = validate_entity(entity_id, entity_type)
                if not validation_result['valid']:
                    results['skipped'].append({
                        'entity_id': entity_id,
                        'reason': validation_result['error']
                    })
                    continue
                
                # Process entity
                processor = EntityProcessor(entity_id, entity_type, data_year)
                success, error_msg = await processor.process()
                
                if success:
                    results['processed'].append({
                        'entity_id': entity_id,
                        'stages_processed': processor.stages
                    })
                else:
                    results['failed'].append({
                        'entity_id': entity_id,
                        'error': error_msg
                    })
                    
            except Exception as e:
                current_app.logger.error(f"Error processing {entity_type} {entity_id}: {str(e)}")
                results['failed'].append({
                    'entity_id': entity_id,
                    'error': str(e)
                })
        
        return jsonify(success_response(results, f"Bulk processing completed: {len(results['processed'])} successful, {len(results['failed'])} failed, {len(results['skipped'])} skipped"))
        
    except Exception as e:
        current_app.logger.error(f"Bulk processing error: {str(e)}", exc_info=True)
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/process/stage/<string:stage>', methods=['POST'])
async def process_single_stage():
    """Process a single stage for an entity"""
    try:
        data = request.get_json()
        entity_id = data.get('entity_id')
        entity_type = data.get('entity_type', 'school')
        stage = request.view_args['stage']
        
        current_app.logger.info(f"Processing stage '{stage}' for {entity_type} {entity_id}")
        
        if not entity_id:
            return jsonify(error_response("entity_id is required")), 400
        
        # Validate entity
        validation_result = validate_entity(entity_id, entity_type)
        if not validation_result['valid']:
            return jsonify(error_response(validation_result['error'])), 400
        
        # Check if stage is applicable
        applicable_stages = validation_result['applicable_stages']
        if stage not in applicable_stages:
            return jsonify(error_response(f"Stage '{stage}' not applicable for this {entity_type}")), 400
        
        # Create processor and process single stage
        processor = EntityProcessor(entity_id, entity_type)
        
        # Initialize Firebase tracking for single stage
        from firebase_admin import firestore
        db = firestore.client()
        doc_id = f"{entity_type}_{entity_id}"
        status_ref = db.collection('entity_processing').document(doc_id)
        
        success = await processor._process_stage(stage, status_ref)
        
        if success:
            return jsonify(success_response({
                'entity_id': entity_id,
                'entity_type': entity_type,
                'stage': stage,
                'status': 'completed'
            }, f"Stage '{stage}' completed successfully"))
        else:
            return jsonify(error_response(f"Stage '{stage}' failed")), 500
            
    except Exception as e:
        current_app.logger.error(f"Single stage processing error: {str(e)}", exc_info=True)
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/process/preview', methods=['POST'])
def preview_processing():
    """Preview what stages would be processed for an entity without actually processing"""
    try:
        data = request.get_json()
        entity_id = data.get('entity_id')
        entity_type = data.get('entity_type', 'school')
        
        if not entity_id:
            return jsonify(error_response("entity_id is required")), 400
        
        # Validate entity and get applicable stages
        validation_result = validate_entity(entity_id, entity_type)
        if not validation_result['valid']:
            return jsonify(error_response(validation_result['error'])), 400
        
        entity_info = validation_result['entity_info']
        applicable_stages = validation_result['applicable_stages']
        
        preview_data = {
            'entity_id': entity_id,
            'entity_type': entity_type,
            'entity_name': entity_info['name'],
            'applicable_stages': applicable_stages,
            'stage_descriptions': {
                'location': 'Validate and geocode location data',
                'demographics': 'Collect ESRI demographic data',
                'enrollment': 'Process enrollment data',
                'projections': 'Generate enrollment projections',
                'metrics': 'Calculate district metrics'
            },
            'requirements': {
                'has_location': entity_info['has_location'],
                'has_enrollment': entity_info['has_enrollment']
            }
        }
        
        return jsonify(success_response(preview_data, "Processing preview generated"))
        
    except Exception as e:
        current_app.logger.error(f"Preview error: {str(e)}", exc_info=True)
        return jsonify(error_response(str(e))), 500 