"""
Task Handlers for Entity Processing

Modular processing functions for different stages of entity processing.
Each handler can work with both schools and location points as applicable.
"""

from .location import process_location_data
from .demographics import process_demographics
from .enrollment import process_enrollment_data
from .projections import process_projections
from .metrics import process_metrics
from .team import process_team_assignment

__all__ = [
    'process_location_data',
    'process_demographics', 
    'process_enrollment_data',
    'process_projections',
    'process_metrics',
    'process_team_assignment'
] 