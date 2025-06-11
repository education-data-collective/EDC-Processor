from flask import Blueprint
unified_bp = Blueprint('unified', __name__)

# Import routes after blueprint creation
from .routes.validation import *   # /validate routes
from .routes.processing import *   # /process-school routes
from .routes.tasks import *        # /tasks/* routes
from .routes.status import *       # Status routes

# Import core functionality
from .processor import UnifiedProcessor
from .task_manager import TaskManager

# Import task handlers for external use
from .task_handlers import (
    process_geocoding,
    process_nces_update,
    process_esri_data,
    process_projections,
    process_metrics
)

__all__ = [
    'unified_bp',
    'UnifiedProcessor',
    'TaskManager',
    'process_geocoding',
    'process_nces_update',
    'process_esri_data',
    'process_projections',
    'process_metrics'
]