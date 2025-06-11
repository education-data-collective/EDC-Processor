from .. import unified_bp
from flask import Blueprint, request, jsonify, current_app
from flask import request, jsonify
from ..task_handlers import (
    process_geocoding,
    process_nces_update,
    process_esri_data,
    process_projections,
    process_metrics
)
from ..utils import success, error
from app import db

@unified_bp.route('/process-geocoding', methods=['POST'])
async def handle_geocoding():
    try:
        payload = request.get_json()
        result = await process_geocoding(payload)
        return jsonify(success(result))
    except Exception as e:
        return error(str(e))

@unified_bp.route('/process-nces', methods=['POST'])
async def handle_nces():
    try:
        payload = request.get_json()
        result = await process_nces_update(payload)
        return jsonify(success(result))
    except Exception as e:
        return error(str(e))

@unified_bp.route('/process-esri', methods=['POST'])
async def handle_esri():
    try:
        payload = request.get_json()
        # Use correct engine
        esri_engine = db.get_engine('esri_data')
        with esri_engine.connect() as conn:
            result = await process_esri_data(payload)
            return jsonify(success(result))
    except Exception as e:
        return error(str(e))

@unified_bp.route('/process-projections', methods=['POST'])
async def handle_projections():
    try:
        payload = request.get_json()
        result = await process_projections(payload)
        return jsonify(success(result))
    except Exception as e:
        return error(str(e))

@unified_bp.route('/process-metrics', methods=['POST'])
async def handle_metrics():
    try:
        payload = request.get_json()
        result = await process_metrics(payload)
        return jsonify(success(result))
    except Exception as e:
        return error(str(e))