"""
School Projection Models

Manages enrollment projections and forecasting data for schools.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base
from .enums import ProjectionType

class SchoolProjection(Base):
    __tablename__ = 'school_projections'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    type = Column(Enum(ProjectionType, values_callable=lambda x: [e.value for e in x]), 
                  nullable=False, server_default='public')
    
    # Frequently accessed fields as columns for performance  
    entry_grade = Column(String(20))
    generated_at = Column(DateTime, nullable=False)
    processing_batch_id = Column(String(50))
    
    # Main projection data as JSONB
    entry_grade_estimates = Column(JSONB)  # {high, low, median, outer_max, outer_min}
    survival_rates = Column(JSONB)         # {oneYear: {Grade 10: 0.98, ...}, threeYear: {...}, fiveYear: {...}}
    forecast_survival_rates = Column(JSONB) # {Grade 10: {max, median, min, outer_min, outer_max}, ...}
    projections = Column(JSONB, nullable=False) # {max: {2024-2025: {Grade 9: 150, ...}}, median: {...}, ...}
    
    # Metadata
    source_data_years = Column(JSONB)      # ["2019-2020", "2020-2021", ...]
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="projections")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'type', name='idx_school_projections_lookup'),
        Index('idx_projections_gin', 'projections', postgresql_using='gin'),
        Index('idx_survival_rates_gin', 'survival_rates', postgresql_using='gin'),
        Index('idx_forecast_survival_gin', 'forecast_survival_rates', postgresql_using='gin'),
        Index('idx_entry_estimates_gin', 'entry_grade_estimates', postgresql_using='gin'),
        Index('idx_public_projections', 'school_id', 'generated_at', 
              postgresql_where="type = 'public'"),
        Index('idx_batch_generated', 'processing_batch_id', 'generated_at'),
        Index('idx_generated_at', 'generated_at'),
        Index('idx_entry_grade', 'entry_grade', 
              postgresql_where='entry_grade IS NOT NULL'),
    ) 