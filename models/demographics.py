"""
Demographic Data Models

Manages ESRI demographic data associated with school locations.
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index, UniqueConstraint, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base

class EsriDemographicData(Base):
    __tablename__ = 'esri_demographic_data'
    
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('location_points.id'), nullable=False)
    drive_time = Column(Integer)
    
    # Age distribution - specific single-year ages (Current Year)
    age4_cy = Column(Float)
    age5_cy = Column(Float)
    age6_cy = Column(Float)
    age7_cy = Column(Float)
    age8_cy = Column(Float)
    age9_cy = Column(Float)
    age10_cy = Column(Float)
    age11_cy = Column(Float)
    age12_cy = Column(Float)
    age13_cy = Column(Float)
    age14_cy = Column(Float)
    age15_cy = Column(Float)
    age16_cy = Column(Float)
    age17_cy = Column(Float)
    
    # Age distribution - specific single-year ages (Future Year)
    age4_fy = Column(Float)
    age5_fy = Column(Float)
    age6_fy = Column(Float)
    age7_fy = Column(Float)
    age8_fy = Column(Float)
    age9_fy = Column(Float)
    age10_fy = Column(Float)
    age11_fy = Column(Float)
    age12_fy = Column(Float)
    age13_fy = Column(Float)
    age14_fy = Column(Float)
    age15_fy = Column(Float)
    age16_fy = Column(Float)
    age17_fy = Column(Float)
    
    # Age distribution - specific single-year ages (2020 Census)
    age4_c20 = Column(Float)
    age5_c20 = Column(Float)
    age6_c20 = Column(Float)
    age7_c20 = Column(Float)
    age8_c20 = Column(Float)
    age9_c20 = Column(Float)
    age10_c20 = Column(Float)
    age11_c20 = Column(Float)
    age12_c20 = Column(Float)
    age13_c20 = Column(Float)
    age14_c20 = Column(Float)
    age15_c20 = Column(Float)
    age16_c20 = Column(Float)
    age17_c20 = Column(Float)
    
    # Adult population by race/ethnicity (2020)
    nhadltwh20 = Column(Float)  # Non-Hispanic White Adults
    nhadltbl20 = Column(Float)  # Non-Hispanic Black Adults
    nhadltas20 = Column(Float)  # Non-Hispanic Asian Adults
    nhadltpi20 = Column(Float)  # Non-Hispanic Pacific Islander Adults
    nhadltai20 = Column(Float)  # Non-Hispanic American Indian Adults
    nhadltot20 = Column(Float)  # Non-Hispanic Other Adults
    nhadlt2_r20 = Column(Float) # Non-Hispanic Two or More Race Adults
    hadults20 = Column(Float)   # Hispanic Adults
    
    # Child population by race/ethnicity (2020)
    nhwu18_c20 = Column(Float)  # Non-Hispanic White Under 18
    nhbu18_c20 = Column(Float)  # Non-Hispanic Black Under 18
    nhasu18_c20 = Column(Float) # Non-Hispanic Asian Under 18
    nhpiu18_c20 = Column(Float) # Non-Hispanic Pacific Islander Under 18
    nhaiu18_c20 = Column(Float) # Non-Hispanic American Indian Under 18
    nhou18_c20 = Column(Float)  # Non-Hispanic Other Under 18
    nhmu18_c20 = Column(Float)  # Non-Hispanic Two or More Race Under 18
    hu18_rbs20 = Column(Float)  # Hispanic Under 18
    
    # Percentage fields for adults (2020)
    per_hisp_adult_20 = Column(Float)  # Percent Hispanic Adult
    per_wht_adult_20 = Column(Float)   # Percent White Adult
    per_blk_adult_20 = Column(Float)   # Percent Black Adult
    per_asn_adult_20 = Column(Float)   # Percent Asian Adult
    per_pi_adult_20 = Column(Float)    # Percent Pacific Islander Adult
    per_ai_adult_20 = Column(Float)    # Percent American Indian Adult
    per_other_adult_20 = Column(Float) # Percent Other Adult
    per_two_or_more_adult_20 = Column(Float) # Percent Two or More Race Adult
    
    # Percentage fields for children (2020)
    per_hisp_child_20 = Column(Float)  # Percent Hispanic Child
    per_wht_child_20 = Column(Float)   # Percent White Child
    per_blk_child_20 = Column(Float)   # Percent Black Child
    per_asn_child_20 = Column(Float)   # Percent Asian Child
    per_pi_child_20 = Column(Float)    # Percent Pacific Islander Child
    per_ai_child_20 = Column(Float)    # Percent American Indian Child
    per_other_child_20 = Column(Float) # Percent Other Child
    per_two_or_more_child_20 = Column(Float) # Percent Two or More Race Child
    
    # Income data - detailed (matching original field names)
    medhinc_cy = Column(Float)         # Median Household Income Current Year
    hincbasecy = Column(Float)         # Household Income Base Current Year
    hinc0_cy = Column(Float)           # Household Income $0-15k Current Year
    hinc15_cy = Column(Float)          # Household Income $15-25k Current Year
    hinc25_cy = Column(Float)          # Household Income $25-35k Current Year
    hinc35_cy = Column(Float)          # Household Income $35-50k Current Year
    per_50k_cy = Column(Float)         # Percent Households Income $50k+ Current Year
    
    # Housing data - detailed (matching original field names)
    tothu_cy = Column(Float)           # Total Housing Units Current Year
    renter_cy = Column(Float)          # Renter Occupied Current Year
    vacant_cy = Column(Float)          # Vacant Units Current Year
    per_renter_cy = Column(Float)      # Percent Renter Occupied Current Year
    per_vacant_cy = Column(Float)      # Percent Vacant Current Year
    
    # ESRI Metadata
    source_country = Column(String(50))
    area_type = Column(String(50))
    buffer_units = Column(String(20))
    buffer_units_alias = Column(String(50))
    buffer_radii = Column(Float)       
    aggregation_method = Column(String(50))
    population_to_polygon_size_rating = Column(Float)  
    apportionment_confidence = Column(Float)           
    has_data = Column(Integer)
    
    # Drive time polygon storage
    drive_time_polygon = Column(Text)  # Store polygon as text/JSON
    
    # Timestamp (from source ESRI data)
    timestamp = Column(DateTime)
    
    # Relationships
    location = relationship("LocationPoint", back_populates="esri_data")
    
    __table_args__ = (
        UniqueConstraint('location_id', 'drive_time', name='uix_esri_location_drive_time'),
        Index('idx_esri_demographic', location_id),
        Index('idx_esri_drive_time', drive_time),  # Useful for drive-time specific queries
    ) 