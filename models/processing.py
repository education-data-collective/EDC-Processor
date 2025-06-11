"""
Processing and Data Management Models

Tracks ETL processing status, data versions, and district-level metrics.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Index, UniqueConstraint, Float, Enum
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
    district_metrics_processed = Column(Boolean, default=False)
    
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

class DistrictMetrics(Base):
    __tablename__ = 'district_metrics'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    
    # Metric data
    total_enrollment = Column(Integer)
    total_schools = Column(Integer)
    average_school_size = Column(Float)
    total_frl_count = Column(Integer)
    frl_percentage = Column(Float)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="district_metrics")
    data_versions = relationship("DistrictMetricDataVersion", back_populates="district_metric")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_district_metric_year'),
        Index('idx_district_metric', school_id, data_year),
    )

class DistrictMetricDataVersion(Base):
    __tablename__ = 'district_metric_data_versions'
    
    id = Column(Integer, primary_key=True)
    district_metric_id = Column(Integer, ForeignKey('district_metrics.id'), nullable=False)
    data_version_id = Column(Integer, ForeignKey('data_versions.id'), nullable=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    district_metric = relationship("DistrictMetrics", back_populates="data_versions")
    data_version = relationship("DataVersion")
    
    __table_args__ = (
        UniqueConstraint('district_metric_id', 'data_version_id', name='uix_district_metric_version'),
        Index('idx_district_metric_version', district_metric_id, data_version_id),
    ) 