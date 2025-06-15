"""
School Projection Models

Manages enrollment projections and forecasting data for schools.
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Enum, Index, UniqueConstraint, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import ProjectionType

class SchoolProjection(Base):
    __tablename__ = 'school_projections'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    projection_type = Column(Enum(ProjectionType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    
    # Projection data
    total = Column(Integer, nullable=False)
    american_indian = Column(Integer)
    asian = Column(Integer)
    black = Column(Integer)
    hispanic = Column(Integer)
    pacific_islander = Column(Integer)
    white = Column(Integer)
    two_or_more_races = Column(Integer)
    frl_count = Column(Integer)
    
    # Data provenance
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    is_user_edited = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="projections")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', 'school_year', 'projection_type', 
                         name='uix_school_projection'),
        Index('idx_school_projection', school_id, data_year),
    ) 