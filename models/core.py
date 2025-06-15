from sqlalchemy import Column, Integer, String, DateTime, Index, text, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base

class School(Base):
    __tablename__ = 'schools'
    
    id = Column(Integer, primary_key=True)
    uuid = Column(String(36), unique=True, nullable=False, server_default=text('gen_random_uuid()'))
    
    # Status tracking
    status = Column(Enum('active', 'inactive', name='school_status'), default='active', nullable=False)
    status_reason = Column(String(255))
    status_changed_at = Column(DateTime, server_default=func.now())
    status_changed_by = Column(String(50))
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Core data relationships
    directory_data = relationship("SchoolDirectoryData", back_populates="school")
    current_directory = relationship("SchoolDirectoryData", 
                                   primaryjoin="and_(School.id==SchoolDirectoryData.school_id, "
                                            "SchoolDirectoryData.is_current==True)",
                                   uselist=False)
    locations = relationship("SchoolLocation", back_populates="school")
    characteristics = relationship("SchoolCharacteristics", back_populates="school")
    grades_offered = relationship("SchoolGradesOffered", back_populates="school")
    enrollments = relationship("SchoolEnrollment", back_populates="school")
    frl = relationship("SchoolFRL", back_populates="school")
    names = relationship("SchoolName", back_populates="school")
    sources = relationship("SchoolSource", back_populates="school")
    
    # Processing and analytics relationships
    processing_status = relationship("ProcessingStatus", back_populates="school")
    projections = relationship("SchoolProjection", back_populates="school")
    school_metrics = relationship("SchoolMetrics", back_populates="school", uselist=False)
    
    # History and audit relationships
    edits = relationship("SchoolEdit", back_populates="school")
    
    # School-to-school relationships
    relationships_source = relationship("SchoolRelationship", 
                                      foreign_keys="SchoolRelationship.source_school_id",
                                      back_populates="source_school")
    relationships_target = relationship("SchoolRelationship", 
                                      foreign_keys="SchoolRelationship.target_school_id",
                                      back_populates="target_school")

    @property
    def name(self):
        """Get display name if available, otherwise system name"""
        if self.names:
            active_name = next((n for n in self.names if n.is_active), None)
            if active_name:
                return active_name.display_name
        
        if self.current_directory:
            return self.current_directory.system_name
        return None 