"""
System Configuration Models

Manages system-wide configuration settings and parameters.
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, UniqueConstraint
from sqlalchemy.sql import func

from .base import Base

class SystemConfiguration(Base):
    __tablename__ = 'system_configurations'
    
    id = Column(Integer, primary_key=True)
    config_key = Column(String(50), nullable=False)
    config_value = Column(String(255), nullable=False)
    description = Column(String(255))
    is_active = Column(Boolean, default=True)
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('config_key', name='uix_system_config_key'),
        Index('idx_system_config_active', is_active),
    ) 