"""
Entity Processing v2

Modern processing system for schools and location points using unified schema.
Supports flexible processing based on entity type and available data.
"""

from flask import Blueprint

# Create the blueprint
entity_bp = Blueprint('entity', __name__)

# Import routes after blueprint creation
from .routes.processing import *
from .routes.validation import *
from .routes.status import *
from .routes.tasks import *

# Import core functionality
from .processor import EntityProcessor
from .task_manager import EntityTaskManager

# Import task handlers for external use
from .task_handlers import (
    process_location_data,
    process_demographics,
    process_enrollment_data,
    process_projections,
    process_metrics,
    process_team_assignment
)

__all__ = [
    'entity_bp',
    'EntityProcessor',
    'EntityTaskManager',
    'process_location_data',
    'process_demographics',
    'process_enrollment_data',
    'process_projections',
    'process_metrics',
    'process_team_assignment'
] 