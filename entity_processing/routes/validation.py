"""
Entity Validation Routes

API endpoints for validating entities before processing.
"""

from flask import request, jsonify, current_app
from .. import entity_bp
from ..utils import validate_entity, success_response, error_response


@entity_bp.route('/validate', methods=['POST'])
def validate_entity_endpoint():
    """Validate a single entity"""
    try:
        data = request.get_json()
        entity_id = data.get('entity_id')
        entity_type = data.get('entity_type', 'school')
        
        if not entity_id:
            return jsonify(error_response("entity_id is required")), 400
        
        validation_result = validate_entity(entity_id, entity_type)
        
        if validation_result['valid']:
            return jsonify(success_response({
                'entity_id': entity_id,
                'entity_type': entity_type,
                'valid': True,
                'entity_info': {
                    'name': validation_result['entity_info']['name'],
                    'has_location': validation_result['entity_info']['has_location'],
                    'has_enrollment': validation_result['entity_info']['has_enrollment']
                },
                'applicable_stages': validation_result['applicable_stages']
            }, "Entity validation successful"))
        else:
            return jsonify(error_response(validation_result['error'])), 400
            
    except Exception as e:
        current_app.logger.error(f"Validation error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/validate/bulk', methods=['POST'])
def validate_bulk_entities():
    """Validate multiple entities"""
    try:
        data = request.get_json()
        entities = data.get('entities', [])
        entity_type = data.get('entity_type', 'school')
        
        if not entities:
            return jsonify(error_response("entities list is required")), 400
        
        results = {
            'valid': [],
            'invalid': [],
            'summary': {
                'total': len(entities),
                'valid_count': 0,
                'invalid_count': 0
            }
        }
        
        for entity_data in entities:
            entity_id = entity_data.get('entity_id') if isinstance(entity_data, dict) else entity_data
            
            try:
                validation_result = validate_entity(entity_id, entity_type)
                
                if validation_result['valid']:
                    results['valid'].append({
                        'entity_id': entity_id,
                        'entity_name': validation_result['entity_info']['name'],
                        'applicable_stages': validation_result['applicable_stages']
                    })
                    results['summary']['valid_count'] += 1
                else:
                    results['invalid'].append({
                        'entity_id': entity_id,
                        'error': validation_result['error']
                    })
                    results['summary']['invalid_count'] += 1
                    
            except Exception as e:
                results['invalid'].append({
                    'entity_id': entity_id,
                    'error': str(e)
                })
                results['summary']['invalid_count'] += 1
        
        return jsonify(success_response(results, f"Validation completed: {results['summary']['valid_count']} valid, {results['summary']['invalid_count']} invalid"))
        
    except Exception as e:
        current_app.logger.error(f"Bulk validation error: {str(e)}")
        return jsonify(error_response(str(e))), 500 