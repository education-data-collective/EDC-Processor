from .. import unified_bp
from flask import request, jsonify, current_app
from ..utils import validate_single_school, validate_schools_structure
from app.utils.auth_helpers import admin_required, firebase_auth_required

# Route to validate a single school
@unified_bp.route('/validate', methods=['POST'])
@firebase_auth_required()
@admin_required
def validate_school():
    """Validate a single school"""
    try:
        data = request.get_json()
        ncessch = data.get('ncessch')
        options = data.get('options', {})
        
        if not ncessch:
            return jsonify({
                'status': 'error',
                'error': 'NCES ID is required'
            }), 400

        result = validate_single_school(ncessch, options)
        return jsonify(result)

    except Exception as e:
        current_app.logger.error(f"Validation error: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Route to validate schools in bulk
@unified_bp.route('/validate/bulk', methods=['POST'])
@firebase_auth_required()
@admin_required
def validate_schools():
    """Validate multiple schools"""
    current_app.logger.info("Starting to validate multiple schools")
    try:
        data = request.get_json()
        schools_data = data.get('schools', [])
        
        if not schools_data:
            return jsonify({
                'status': 'error',
                'error': 'No schools provided for validation'
            }), 400

        results = validate_schools_structure(schools_data)
        current_app.logger.info("Finished validating multiple schools")
        current_app.logger.info(f"Validation Results: {results}")
        return jsonify(results)

    except Exception as e:
        current_app.logger.error(f"Validation error: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500