from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base

class SchoolName(Base):
    __tablename__ = 'school_names'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    display_name = Column(String(100), nullable=False)  # Standardized name with proper capitalization for display
    source_name = Column(String(100))  # Original name this standardization is based on
    reason = Column(String(255))  # Why this standardization was applied
    is_active = Column(Boolean, default=True)
    created_by = Column(String(50), nullable=False)  # system or username
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="names")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_school_name_year'),
        Index('idx_school_names', school_id),
        Index('idx_school_names_active', is_active),
        Index('idx_school_names_year', data_year),
    ) 