"""
Processing and Data Management Models

Tracks ETL processing status, data versions, and school-level metrics.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Index, UniqueConstraint, Float, Enum, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import DataCompleteness

class ProcessingStatus(Base):
    __tablename__ = 'processing_status'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    
    # Processing flags
    enrollment_processed = Column(Boolean, default=False)
    location_processed = Column(Boolean, default=False)
    characteristics_processed = Column(Boolean, default=False)
    projections_processed = Column(Boolean, default=False)
    demographics_processed = Column(Boolean, default=False)
    nces_processed = Column(Boolean, default=False)
    geocoding_processed = Column(Boolean, default=False)
    esri_processed = Column(Boolean, default=False)
    school_metrics_processed = Column(Boolean, default=False)
    
    # Data completeness
    data_completeness = Column(Enum(DataCompleteness, values_callable=lambda x: [e.value for e in x]), nullable=True)
    
    # Timestamps
    last_processed_at = Column(DateTime)
    nces_processed_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="processing_status")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_processing_status_year'),
        Index('idx_processing_status', school_id, data_year),
    )

class DataVersion(Base):
    __tablename__ = 'data_versions'
    
    id = Column(Integer, primary_key=True)
    version_name = Column(String(50), nullable=True)
    version = Column(String(50), nullable=False)
    version_description = Column(String(255))
    is_current = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    is_comparison = Column(Boolean, default=False)
    dataset_name = Column(String(50), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9))
    import_date = Column(DateTime, nullable=False)
    notes = Column(String(255))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('version', name='uix_data_version'),
        Index('idx_data_version_current', is_current),
        Index('idx_data_version_dataset', dataset_name, data_year),
    )

class SchoolMetrics(Base):
    __tablename__ = 'school_metrics'
    
    school_id = Column(Integer, ForeignKey('schools.id'), primary_key=True, nullable=False)
    drive_time = Column(Integer, primary_key=True, nullable=False)  # Drive time in minutes (e.g., 5, 10, 15)
    calculated_at = Column(DateTime, nullable=False)
    data_versions = Column(JSON)  # Tracks data versions used in calculation
    
    # Population demographics
    population_past = Column(Integer)  # Historical population for school grades
    population_current = Column(Integer)  # Current population for school grades
    population_future = Column(Integer)  # Projected population for school grades
    population_trend_past_to_latest = Column(Float)  # Percentage change in population
    population_trend_latest_to_projected = Column(Float)  # Projected population change
    population_trend_status = Column(String(20))  # growing, declining, or stable
    population_projection_status = Column(String(20))  # projected trend status
    
    # Market share analysis
    market_share_past = Column(Float)  # Historical market share percentage
    market_share_current = Column(Float)  # Current market share percentage
    market_share_trend = Column(Float)  # Change in market share
    market_share_status = Column(String(20))  # gaining, losing, or stable
    
    # Enrollment metrics
    enrollment_past = Column(Integer)  # Historical total enrollment
    enrollment_current = Column(Integer)  # Current total enrollment
    public_enrollment_projected = Column(Integer)  # Public projection model result
    updated_enrollment_projected = Column(Integer)  # Updated projection model result
    projection_type = Column(String(20))  # none, public, or updated
    enrollment_trend_past_to_latest = Column(Float)  # Enrollment percentage change
    enrollment_trend_latest_to_projected = Column(Float)  # Projected enrollment change
    enrollment_trend_status = Column(String(20))  # growing, declining, or stable
    enrollment_projection_status = Column(String(20))  # projected enrollment trend
    
    # School characteristics
    is_newer = Column(Boolean, default=False)  # True if school is considered new/emerging
    has_projections = Column(Boolean, default=False)  # True if projection data available
    
    # Relationships
    school = relationship("School", back_populates="school_metrics")
    data_version_links = relationship("SchoolMetricDataVersion", back_populates="school_metric")
    
    __table_args__ = (
        Index('idx_school_metrics_school', school_id),
        Index('idx_school_metrics_drive_time', drive_time),
        Index('idx_school_metrics_calculated', calculated_at),
        Index('idx_school_metrics_trend', enrollment_trend_status),
        Index('idx_school_metrics_market', market_share_status),
    )

class SchoolMetricDataVersion(Base):
    __tablename__ = 'school_metric_data_versions'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, nullable=False)
    drive_time = Column(Integer, nullable=False)
    data_version_id = Column(Integer, ForeignKey('data_versions.id'), nullable=False)
    metric_type = Column(String(50))  # enrollment, demographics, projections, etc.
    
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    school_metric = relationship("SchoolMetrics", back_populates="data_version_links")
    data_version = relationship("DataVersion")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'drive_time', 'data_version_id', 'metric_type', name='uix_school_metric_version'),
        Index('idx_school_metric_versions_school_drive', school_id, drive_time),
        Index('idx_school_metric_versions_data', data_version_id),
    ) 