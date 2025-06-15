from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Enum, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base
from .enums import SchoolType, VirtualInstruction

class SchoolCharacteristics(Base):
    __tablename__ = 'school_characteristics'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    
    # Educational/operational characteristics
    school_type = Column(Enum(SchoolType, values_callable=lambda x: [e.value for e in x]), 
                        default=SchoolType.REGULAR_SCHOOL)
    teachers = Column(Float)
    virtual = Column(Enum(VirtualInstruction, values_callable=lambda x: [e.value for e in x]), 
                    default=VirtualInstruction.NO_VIRTUAL_INSTRUCTION)
    website = Column(String(255), default='Not available')
    
    # Religious information for private schools
    religious_affiliation = Column(String(50), default='None')
    religious_orientation = Column(String(100), default='None')
    
    # Data provenance
    source_id = Column(Integer, ForeignKey('school_sources.id'))
    is_user_edited = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="characteristics")
    source = relationship("SchoolSource")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', name='uix_school_characteristics_year'),
        Index('idx_school_characteristics', school_id),
        Index('idx_school_characteristics_type', school_type),
        Index('idx_school_characteristics_virtual', virtual),
    )

class SchoolGradesOffered(Base):
    __tablename__ = 'school_grades_offered'
    
    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, ForeignKey('schools.id'), nullable=False)
    data_year = Column(Integer, nullable=False)
    school_year = Column(String(9), nullable=False)
    grade = Column(String(2), nullable=False)  # Format: PK, KG, 01-12
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    school = relationship("School", back_populates="grades_offered")
    
    __table_args__ = (
        UniqueConstraint('school_id', 'data_year', 'grade', name='uix_school_grade_year'),
        Index('idx_school_grades', school_id),
        Index('idx_school_grades_year', data_year),
        Index('idx_grade', grade),
    ) 