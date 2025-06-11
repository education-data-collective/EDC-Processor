"""
Entity Processing Task Routes

API endpoints for individual task handlers (used by task manager or direct calls).
"""

from flask import request, jsonify, current_app
from .. import entity_bp
from ..task_handlers import (
    process_location_data,
    process_demographics,
    process_enrollment_data,
    process_projections,
    process_metrics,
    process_team_assignment
)
from ..utils import success_response, error_response


@entity_bp.route('/tasks/location', methods=['POST'])
async def handle_location_processing():
    """Handle location processing task"""
    try:
        payload = request.get_json()
        result = await process_location_data(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Location task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/demographics', methods=['POST'])
async def handle_demographics_processing():
    """Handle demographics processing task"""
    try:
        payload = request.get_json()
        result = await process_demographics(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Demographics task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/enrollment', methods=['POST'])
async def handle_enrollment_processing():
    """Handle enrollment processing task"""
    try:
        payload = request.get_json()
        result = await process_enrollment_data(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Enrollment task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/projections', methods=['POST'])
async def handle_projections_processing():
    """Handle projections processing task"""
    try:
        payload = request.get_json()
        result = await process_projections(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Projections task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/metrics', methods=['POST'])
async def handle_metrics_processing():
    """Handle metrics processing task"""
    try:
        payload = request.get_json()
        result = await process_metrics(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Metrics task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/team', methods=['POST'])
async def handle_team_assignment():
    """Handle team assignment task"""
    try:
        payload = request.get_json()
        result = await process_team_assignment(payload)
        return jsonify(result)
    except Exception as e:
        current_app.logger.error(f"Team assignment task error: {str(e)}")
        return jsonify(error_response(str(e))), 500


@entity_bp.route('/tasks/health', methods=['GET'])
def task_health_check():
    """Health check for task endpoints"""
    try:
        return jsonify(success_response({
            'status': 'healthy',
            'available_tasks': [
                'location',
                'demographics', 
                'enrollment',
                'projections',
                'metrics',
                'team'
            ]
        }, "Task endpoints are healthy"))
    except Exception as e:
        current_app.logger.error(f"Health check error: {str(e)}")
        return jsonify(error_response(str(e))), 500 