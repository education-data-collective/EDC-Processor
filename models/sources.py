from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import DataSource

class SchoolSource(Base):
    __tablename__ = 'school_sources'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    source_type = Column(Enum(DataSource, values_callable=lambda x: [e.value for e in x]), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="sources")
    
    __table_args__ = (
        Index('idx_school_source', school_id),
        Index('idx_school_source_type', source_type),
    ) 