from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import SchoolStatus, SchoolOwnership, SchoolOperationalModel

class SchoolDirectoryData(Base):
    __tablename__ = 'school_directory'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    
    # Directory data that changes over time
    ncessch = Column(String(12))
    split_suffix = Column(String(3))  # Split school suffix (es, ms, hs) - NULL for non-split schools
    state_school_id = Column(String(50))  # ST_SCHID from NCES data
    system_name = Column(String(100), nullable=False)  # Original name as provided by data source
    lea_name = Column(String(100))
    state_name = Column(String(50))
    state_abbr = Column(String(2))
    status = Column(Enum(SchoolStatus, values_callable=lambda x: [e.value for e in x]), 
                   nullable=False, default=SchoolStatus.OPEN)
    ownership = Column(Enum(SchoolOwnership, values_callable=lambda x: [e.value for e in x]))
    operational_model = Column(Enum(SchoolOperationalModel, values_callable=lambda x: [e.value for e in x]))
    
    # Current record indicator
    is_current = Column(Boolean, default=False)
    
    # Data provenance
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="directory_data")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_school_directory_year'),
        Index('idx_school_directory_current', school_id, is_current),
        Index('idx_school_directory_ncessch', ncessch),
        Index('idx_school_directory_split_suffix', split_suffix),
        Index('idx_school_directory_ncessch_suffix', ncessch, split_suffix),
        Index('idx_school_directory_state_school_id', state_school_id),
        Index('idx_school_directory_year', data_year),
        Index('idx_school_directory_status', status),
        Index('idx_school_directory_ownership_model', ownership, operational_model),
    )

    @property
    def is_private(self):
        """Check if school is private based on ownership field"""
        return self.ownership == SchoolOwnership.PRIVATE if self.ownership else False
    
    @property
    def is_charter(self):
        """Check if school is a charter school based on operational model"""
        return self.operational_model == SchoolOperationalModel.CHARTER if self.operational_model else False
    
    @property
    def is_private_charter(self):
        """Check if school is a private charter school"""
        return self.is_private and self.is_charter
    
    @property
    def full_ncessch(self):
        """Get the full NCESSCH including split suffix if applicable"""
        if self.split_suffix:
            return f"{self.ncessch}-{self.split_suffix}"
        return self.ncessch 