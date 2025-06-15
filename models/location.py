from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base

class LocationPoint(Base):
    __tablename__ = 'location_points'
    
    id = Column(Integer, primary_key=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    address = Column(String(200))
    city = Column(String(50))
    county = Column(String(50))
    state = Column(String(2))
    zip_code = Column(String(10))
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    school_locations = relationship("SchoolLocation", back_populates="location_point")
    polygon_relationships = relationship("SchoolPolygonRelationship", back_populates="location")
    esri_data = relationship("EsriDemographicData", back_populates="location")
    
    __table_args__ = (
        Index('idx_location_coordinates', latitude, longitude),
    )

class SchoolLocation(Base):
    __tablename__ = 'school_locations'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    location_id = Column(Integer, ForeignKey('location_points.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9))
    is_current = Column(Boolean, default=True)
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    changed_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="locations")
    location_point = relationship("LocationPoint", back_populates="school_locations")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_school_location_year'),
        Index('idx_school_location_current', school_id, is_current),
        Index('idx_school_location', school_id),
        Index('idx_school_location_point', location_id),
    ) 