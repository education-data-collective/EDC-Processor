# 📊 Enrollment Projections Module

This module generates grade-level enrollment projections for schools using historical enrollment data. The module follows a two-phase approach: analysis and processing.

## 🎯 Overview

The projections module identifies schools with sufficient historical data and generates population projections for:
- **Public schools**: Using 2023 data year
- **Private schools**: Using 2021 data year

Projections include multiple scenarios (min/median/max/outer bounds) and are stored both as CSV files and in the `school_projections` database table.

## 📁 Module Structure

```
05_projections/
├── 01_projections_analysis.py     # Analysis phase - identifies projection-ready schools
├── 02_projections_processing.py   # Processing phase - generates populations (coming next)
├── README.md                      # This documentation
├── tests/                         # Test files
│   ├── test_edge_cases.py
│   ├── test_standalone_projections.py
│   └── test_projections_comparison.py
├── 01_projections/               # Analysis outputs
│   └── projection_ready_*.csv    # Schools ready for processing
└── output/                       # Generated populations
    ├── populations_public_2023_TIMESTAMP.csv
    └── populations_private_2021_TIMESTAMP.csv
```

## 🚀 Usage

### Phase 1: Analysis
```bash
cd 05_projections
python 01_projections_analysis.py
```

**What it does:**
- Analyzes schools with 2023 data (public) and 2021 data (private)
- Identifies schools with sufficient historical enrollment data (≥3 years)
- Performs data quality assessment
- Exports lists of projection-ready schools
- Generates processing recommendations

**Output:**
- `01_projections/projection_ready_public_TIMESTAMP.csv`
- `01_projections/projection_ready_private_TIMESTAMP.csv`
- Console report with statistics and recommendations

### Phase 2: Processing
```bash
cd 05_projections
python 02_projections_processing.py
```

**What it does:**
- Processes schools identified in Phase 1
- Generates grade-level enrollment projections for 5 future years
- Creates multiple projection scenarios (min/median/max/outer bounds)
- Stores results in database and exports CSV files

**Output:**
- `output/populations_public_2023_TIMESTAMP.csv`
- `output/populations_private_2021_TIMESTAMP.csv`
- Records in `school_projections` database table

## 📊 Data Requirements

### Minimum Requirements
- **3+ years** of historical enrollment data
- **Positive enrollment values** (negative values are filtered out)
- **Grade-level data** (not just school totals)

### Optimal Requirements
- **5+ years** of historical enrollment data for higher confidence projections
- **Consistent grade offerings** across years
- **Complete demographic breakdowns** (when available)

## 🎯 Projection Methodology

The module uses sophisticated survival rate analysis:

1. **Historical Analysis**: Examines 3-5 years of enrollment data
2. **Survival Rates**: Calculates grade-to-grade transition rates
3. **Entry Grade Estimation**: Models kindergarten/entry grade enrollment
4. **Multi-Scenario Projections**: Generates conservative, median, and optimistic projections
5. **Outer Bounds**: Uses historical min/max as bounds checking

## 📈 Output Format

### CSV Structure
Each row represents a projection for one school/grade/year/scenario:

| Column | Description | Example |
|--------|-------------|---------|
| school_id | Internal school ID | 132 |
| ncessch | NCES school ID | "010019702432" |
| school_name | School name | "Example Elementary" |
| projection_year | Future school year | "2024-2025" |
| grade | Grade level | "Grade 5" |
| projection_type | Scenario type | "median" |
| projected_enrollment | Projected count | 23 |
| entry_grade | School's entry grade | "Kindergarten" |
| methodology | Method used | "survival_rate" |

### Database Structure
Stored in `school_projections` table as JSON:
- **One row per school** (not per projection)
- **JSON fields** for projections, survival rates, estimates
- **Optimized for queries** with GIN indexes

## 🔍 Quality Assurance

### Data Validation
- Checks for negative or zero enrollments
- Validates grade progression consistency  
- Identifies schools with insufficient data
- Flags unusual enrollment patterns

### Projection Validation
- Compares projections against historical bounds
- Validates survival rate calculations
- Checks for logical grade progressions
- Generates confidence indicators

## ⚠️ Common Issues

### Data Quality Issues
- **Missing years**: Some schools may have gaps in enrollment data
- **Grade changes**: Schools may add/drop grades over time
- **Enrollment spikes**: Unusual enrollment changes may affect projections

### Resolution Strategies
- **Fallback methods**: Use historical medians when survival rates unavailable
- **Confidence levels**: Lower confidence for schools with limited data
- **Manual review**: Flag unusual projections for review

## 🛠️ Configuration

### Key Parameters
```python
PUBLIC_DATA_YEAR = 2023          # Public schools data year
PRIVATE_DATA_YEAR = 2021         # Private schools data year
MIN_YEARS_FOR_PROJECTIONS = 3    # Minimum years required
OPTIMAL_YEARS_FOR_PROJECTIONS = 5 # Optimal years for high confidence
```

### Database Configuration
- Uses Cloud SQL with proxy connection
- Requires `school_enrollments` table for historical data
- Stores results in `school_projections` table

## 📞 Support

For issues or questions:
1. Check the console output for specific error messages
2. Review the generated CSV files for data quality issues
3. Validate database connectivity and permissions
4. Check historical enrollment data completeness

## 🔄 Version History

- **v2.0**: JSON-based storage in PostgreSQL, two-phase workflow
- **v1.0**: Original Firebase-based projections system 