from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import FRLDataGroup, FRLLunchProgram, FRLTotalIndicator, FRLDMSFlag

class SchoolEnrollment(Base):
    __tablename__ = 'school_enrollments'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    grade = Column(String(2), nullable=False)  # Format: PK, KG, 01-12
    
    # Enrollment counts by demographics
    total = Column(Integer, nullable=False)
    american_indian = Column(Integer)
    asian = Column(Integer)
    black = Column(Integer)
    hispanic = Column(Integer)
    pacific_islander = Column(Integer)
    white = Column(Integer)
    two_or_more_races = Column(Integer)
    
    # Data provenance
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    is_user_edited = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="enrollments")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', 'school_year', 'grade', name='uix_school_enrollment'),
        Index('idx_school_enrollment', school_id),
        Index('idx_school_enrollment_year', data_year),
        Index('idx_enrollment_grade', grade),
        Index('idx_school_enrollment_school_year', school_id, data_year),
    )

class SchoolFRL(Base):
    __tablename__ = 'school_frl'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    
    # Free/Reduced Lunch data - not provided by private schools
    frl_count = Column(Integer)
    dms_flag = Column(Enum(FRLDMSFlag, values_callable=lambda x: [e.value for e in x]))
    data_group = Column(Enum(FRLDataGroup, values_callable=lambda x: [e.value for e in x]))
    lunch_program = Column(Enum(FRLLunchProgram, values_callable=lambda x: [e.value for e in x]))
    total_indicator = Column(Enum(FRLTotalIndicator, values_callable=lambda x: [e.value for e in x]))
    
    # Data provenance
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    is_user_edited = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="frl")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_school_frl_year'),
        Index('idx_school_frl', school_id),
        Index('idx_school_frl_year', data_year),
        Index('idx_school_frl_program', lunch_program),
        Index('idx_school_frl_data_group', data_group),
    ) 