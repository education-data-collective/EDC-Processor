"""
History and Audit Models

Tracks changes and edits made to school data over time.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import EditType

class SchoolEdit(Base):
    __tablename__ = 'school_edits'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    edit_type = Column(Enum(EditType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    field_name = Column(String(50), nullable=False)
    old_value = Column(String(255))
    new_value = Column(String(255))
    edited_by = Column(String(50), nullable=False)
    edit_reason = Column(String(255))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    school = relationship("School", back_populates="edits")
    
    __table_args__ = (
        Index('idx_school_edit', school_id, edit_type),
        Index('idx_school_edit_timestamp', created_at),
    ) 