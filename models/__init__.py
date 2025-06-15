"""
ETL v2 Models Package

This package contains SQLAlchemy models for the educational data system.
Includes all core ETL models plus additional analytics, relationships, and processing models.
"""

from .base import Base
from .enums import *
from .core import School
from .directory import SchoolDirectoryData
from .location import LocationPoint, SchoolLocation
from .characteristics import SchoolCharacteristics, SchoolGradesOffered
from .enrollment import SchoolEnrollment, SchoolFRL
from .names import SchoolName
from .sources import SchoolSource

# Additional integrated models
from .processing import ProcessingStatus, DataVersion, SchoolMetrics, SchoolMetricDataVersion
from .relationships import SchoolRelationship, RelationshipAttribute, SchoolPolygonRelationship, NearbySchoolPolygon
from .demographics import EsriDemographicData
from .projections import SchoolProjection
from .history import SchoolEdit
from .configuration import SystemConfiguration

__all__ = [
    'Base',
    # Enums
    'SchoolStatus', 'SchoolOwnership', 'SchoolOperationalModel',
    'SchoolType', 'VirtualInstruction',
    'FRLDataGroup', 'FRLLunchProgram', 'FRLTotalIndicator', 'FRLDMSFlag',
    'DataSource', 'DataCompleteness',
    'RelationshipType', 'ProjectionType', 'EditType',
    
    # Core Models
    'School',
    'SchoolDirectoryData',
    'LocationPoint',
    'SchoolLocation',
    'SchoolCharacteristics',
    'SchoolGradesOffered',
    'SchoolEnrollment',
    'SchoolFRL',
    'SchoolName',
    'SchoolSource',
    
    # Processing and Data Management
    'ProcessingStatus',
    'DataVersion',
    'SchoolMetrics',
    'SchoolMetricDataVersion',
    
    # Relationships and Spatial
    'SchoolRelationship',
    'RelationshipAttribute',
    'SchoolPolygonRelationship',
    'NearbySchoolPolygon',
    
    # Analytics and Demographics
    'EsriDemographicData',
    'SchoolProjection',
    
    # History and Configuration
    'SchoolEdit',
    'SystemConfiguration',
] 