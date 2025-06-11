import enum

# Core school enums
class SchoolStatus(enum.Enum):
    """School operational status based on NCES SY_STATUS_TEXT and UPDATED_STATUS_TEXT"""
    OPEN = "open"
    CLOSED = "closed"
    NEW = "new"
    REOPENED = "reopened"
    INACTIVE = "inactive"
    ADDED = "added"
    CHANGED_BOUNDARY_AGENCY = "changed_boundary_agency"
    FUTURE = "future"

class SchoolOwnership(enum.Enum):
    """School ownership/funding model"""
    PUBLIC = "public"      # Publicly funded
    PRIVATE = "private"    # Privately funded

class SchoolOperationalModel(enum.Enum):
    """School operational/governance model"""
    TRADITIONAL = "traditional"    # Traditional public/private school
    CHARTER = "charter"           # Charter school (can be public or private)

class SchoolType(enum.Enum):
    """NCES school type classifications"""
    ALTERNATIVE_SCHOOL = "alternative_school"
    CAREER_AND_TECHNICAL_SCHOOL = "career_and_technical_school"
    REGULAR_SCHOOL = "regular_school"
    SPECIAL_EDUCATION_SCHOOL = "special_education_school"

class VirtualInstruction(enum.Enum):
    """Virtual instruction models"""
    EXCLUSIVELY_VIRTUAL = "exclusively_virtual"
    PRIMARILY_VIRTUAL = "primarily_virtual"
    SUPPLEMENTAL_VIRTUAL = "supplemental_virtual"
    NO_VIRTUAL_INSTRUCTION = "no_virtual_instruction"
    MISSING = "missing"
    NOT_APPLICABLE = "not_applicable"

# FRL (Free/Reduced Lunch) enums
class FRLDataGroup(enum.Enum):
    """Free/Reduced Lunch data collection method"""
    DIRECT_CERTIFICATION = "direct_certification"
    FREE_AND_REDUCED_PRICE_LUNCH_TABLE = "free_and_reduced_price_lunch_table"

class FRLLunchProgram(enum.Enum):
    """Type of lunch program qualification"""
    FREE_LUNCH_QUALIFIED = "free_lunch_qualified"
    MISSING = "missing"
    NO_CATEGORY_CODES = "no_category_codes"
    NOT_APPLICABLE = "not_applicable"
    REDUCED_PRICE_LUNCH_QUALIFIED = "reduced_price_lunch_qualified"

class FRLTotalIndicator(enum.Enum):
    """Data aggregation level indicator"""
    CATEGORY_SET_A = "category_set_a"
    EDUCATION_UNIT_TOTAL = "education_unit_total"

class FRLDMSFlag(enum.Enum):
    """Data quality/source indicator for FRL data"""
    DERIVED = "derived"
    IMPUTATION = "imputation"
    MANUAL_ADJUSTMENT = "manual_adjustment"
    MISSING = "missing"
    NOT_APPLICABLE = "not_applicable"
    NOT_REPORTED = "not_reported"
    POST_SUBMISSION_EDIT = "post_submission_edit"
    REPORTED = "reported"
    SUPPRESSED = "suppressed"

# Data source and management enums
class DataSource(enum.Enum):
    """Data source types"""
    NCES = "nces"
    MANUAL = "manual"
    DERIVED = "derived"
    SPLIT = "split"

class DataCompleteness(enum.Enum):
    """Data completeness levels"""
    COMPLETE = "complete"  # 5+ years of data from any source
    PARTIAL = "partial"    # 1-4 years of data from any source
    NONE = "none"          # No enrollment data

# Relationship enums
class RelationshipType(enum.Enum):
    """School relationship types"""
    SPLIT = "split"
    NEARBY = "nearby"
    PARENT_CHILD = "parent_child"

# Projection and analysis enums
class ProjectionType(enum.Enum):
    """Enrollment projection types"""
    PUBLIC = "public"
    UPDATED = "updated"

class EditType(enum.Enum):
    """School data edit types"""
    NAME = "name"
    ADDRESS = "address"
    ENROLLMENT = "enrollment"
    ATTRIBUTE = "attribute"
    PROJECTION = "projection" 