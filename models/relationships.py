"""
School Relationship Models

Manages relationships between schools and spatial polygon relationships.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import RelationshipType

class SchoolRelationship(Base):
    __tablename__ = 'school_relationships'
    
    id = Column(Integer, primary_key=True)
    source_school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    target_school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    relationship_type = Column(Enum(RelationshipType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    split_type = Column(String(10))
    
    # Temporal data
    effective_from = Column(DateTime)
    effective_to = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    source_school = relationship("School", foreign_keys=[source_school_id], back_populates="relationships_source")
    target_school = relationship("School", foreign_keys=[target_school_id], back_populates="relationships_target")
    attributes = relationship("RelationshipAttribute", back_populates="relationship")
    
    __table_args__ = (
        UniqueConstraint('source_school_id', 'target_school_id', 'relationship_type', 
                         name='uix_school_relationship'),
        Index('idx_school_relationship_source', source_school_id),
        Index('idx_school_relationship_target', target_school_id),
    )

class RelationshipAttribute(Base):
    __tablename__ = 'relationship_attributes'
    
    id = Column(Integer, primary_key=True)
    relationship_id = Column(Integer, ForeignKey('school_relationships.id'), nullable=False)
    attribute_name = Column(String(50), nullable=False)
    attribute_value = Column(String(255), nullable=False)
    attribute_type = Column(String(20))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    relationship = relationship("SchoolRelationship", back_populates="attributes")
    
    __table_args__ = (
        UniqueConstraint('relationship_id', 'attribute_name', name='uix_relationship_attribute'),
        Index('idx_relationship_attribute', relationship_id),
    )

class SchoolPolygonRelationship(Base):
    __tablename__ = 'school_polygon_relationships'
    
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('location_points.id'), nullable=False)
    drive_time = Column(Integer, nullable=False)
    data_year = Column(Integer, nullable=False)
    
    # Metadata
    processed_at = Column(DateTime, server_default=func.now())
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    location = relationship("LocationPoint", back_populates="polygon_relationships")
    nearby_schools = relationship("NearbySchoolPolygon", back_populates="polygon_relationship")
    
    __table_args__ = (
        UniqueConstraint('location_id', 'drive_time', 'data_year', 
                         name='uix_polygon_relationship_year'),
        Index('idx_polygon_location_year', location_id, data_year),
    )

class NearbySchoolPolygon(Base):
    __tablename__ = 'nearby_school_polygons'
    
    id = Column(Integer, primary_key=True)
    polygon_relationship_id = Column(Integer, ForeignKey('school_polygon_relationships.id'), nullable=False)
    school_uuid = Column(String(36), nullable=False)
    relationship_type = Column(String(20), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    polygon_relationship = relationship("SchoolPolygonRelationship", back_populates="nearby_schools")
    
    __table_args__ = (
        UniqueConstraint('polygon_relationship_id', 'school_uuid', 'relationship_type', 
                         name='uix_nearby_school_polygon'),
        Index('idx_nearby_school_polygon_relationship', polygon_relationship_id),
        Index('idx_nearby_school_polygon_uuid', school_uuid),
    ) 