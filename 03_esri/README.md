# ESRI Demographic Data Processing

This module processes ESRI demographic data for EDC schools, integrating with the EDC Unified Database. It analyzes coverage and processes demographic data for schools from the Firebase EDC schools dataset.

## Files Overview

- **`01_esri_analysis.py`** - Analyzes ESRI demographic data coverage for EDC schools
- **`02_esri_processing.py`** - Processes ESRI demographic data for EDC schools and updates processing_status
- **`fetch.py`** - Core ESRI data fetching functionality from ArcGIS API
- **`output/`** - Directory for analysis output files

## Quick Start

### 1. Prerequisites

Make sure you have:
- Cloud SQL Proxy installed (`cloud-sql-proxy` or `cloud_sql_proxy`)
- Service account key file: `../etl-service-account-key.json`
- Python dependencies: `sqlalchemy`, `psycopg2`, `arcgis`, `shapely`, `pandas`
- ESRI credentials in `.env` file
- EDC schools CSV file: `../edc_schools/firebase_schools_06152025.csv`

### 2. Install Dependencies

```bash
pip install sqlalchemy psycopg2-binary arcgis shapely python-dotenv pandas
```

### 3. Set up ESRI Credentials

Create a `.env` file in the project root:
```bash
ESRI_USERNAME=your_username_or_secret_ref
ESRI_PASSWORD=your_password_or_secret_ref
ESRI_URL=https://www.arcgis.com
```

### 4. Usage

#### Analyze ESRI Coverage for EDC Schools
```bash
python 03_esri/01_esri_analysis.py
```

This will:
- Analyze current ESRI demographic data coverage
- Show breakdown by drive time (5, 10, 15 minutes)
- Identify EDC schools missing ESRI data
- Export detailed analysis to CSV files

#### Process ESRI Data for EDC Schools
```bash
# Process schools needing ESRI data (respects 30-day cache)
python 03_esri/02_esri_processing.py

# Force refresh all EDC schools
python 03_esri/02_esri_processing.py --force-refresh

# Process with limits (for testing)
python 03_esri/02_esri_processing.py --limit 10

# Use specific data year for processing_status updates
python 03_esri/02_esri_processing.py --data-year 2023
```

This will:
- Find EDC schools needing ESRI processing
- Fetch demographic data from ESRI API
- Store data in `esri_demographic_data` table
- Validate data completeness
- Update `processing_status.esri_processed` flag

## Database Schema

The processor uses the `esri_demographic_data` table with the following structure:

### Core Fields
- `id` - Primary key
- `location_id` - Foreign key to location_points table
- `latitude`, `longitude` - Coordinates
- `drive_time` - Drive time in minutes (5, 10, or 15)
- `processed_at` - Timestamp of processing
- `has_data` - Boolean flag for data availability

### Age Demographics (Current Year)
- `age4_cy` through `age17_cy` - Population counts by age

### Age Demographics (Future Year) 
- `age4_fy` through `age17_fy` - Future population projections

### Age Demographics (2020 Census)
- `age4_c20` through `age17_c20` - 2020 census data

### Adult Racial/Ethnic Percentages (2020)
- `per_hisp_adult_20` - Hispanic adult percentage
- `per_wht_adult_20` - White adult percentage  
- `per_blk_adult_20` - Black adult percentage
- `per_asn_adult_20` - Asian adult percentage
- `per_pi_adult_20` - Pacific Islander adult percentage
- `per_ai_adult_20` - American Indian adult percentage
- `per_other_adult_20` - Other race adult percentage
- `per_two_or_more_adult_20` - Two or more races adult percentage

### Child Racial/Ethnic Percentages (2020)
- `per_hisp_child_20` - Hispanic child percentage
- `per_wht_child_20` - White child percentage
- `per_blk_child_20` - Black child percentage
- `per_asn_child_20` - Asian child percentage
- `per_pi_child_20` - Pacific Islander child percentage
- `per_ai_child_20` - American Indian child percentage
- `per_other_child_20` - Other race child percentage
- `per_two_or_more_child_20` - Two or more races child percentage

### Economic Data
- `medhinc_cy` - Median household income (current year)
- `per_50k_cy` - Percentage of households earning <$50K
- `per_renter_cy` - Percentage of renter-occupied units
- `per_vacant_cy` - Percentage of vacant housing units

### Spatial Data
- `drive_time_polygon` - JSONB polygon geometry for the drive-time area

## Data Processing Flow

1. **EDC School Loading**: Loads school IDs from Firebase EDC schools CSV
2. **Coverage Analysis**: Identifies schools with missing/incomplete ESRI data
3. **Coordinate Lookup**: Fetches lat/lon from school locations via `location_points` table
4. **ESRI API Call**: Uses ArcGIS to get demographic data for 5/10/15 minute drive times
5. **Data Transformation**: Calculates percentages and validates data
6. **Database Storage**: Saves to `esri_demographic_data` table with proper constraints
7. **Status Update**: Updates `processing_status.esri_processed` flag
8. **Caching**: Avoids re-processing recent data (within 30 days) unless forced

## Processing Status Integration

The processing updates the `processing_status` table:

- **`esri_processed = true`**: School has complete ESRI data (all 3 drive times with valid data)
- **`esri_processed = false`**: School missing ESRI data or processing failed
- **`last_processed_at`**: Updated when processing attempts are made

## Error Handling

The processor includes comprehensive error handling:
- Database connection failures
- ESRI API errors
- Data validation issues
- Transaction rollbacks on failures
- Detailed logging throughout the process

## Performance Considerations

- **Caching**: Recent data (within 30 days) is not re-processed unless `--force-refresh` is used
- **Rate Limiting**: 2-second delays between API calls to respect ESRI limits
- **Transactions**: Atomic operations ensure data consistency
- **Indexing**: Database indexes on location_id, drive_time, and processed_at

## Output Files

Analysis generates CSV files in the `output/` directory:

- `esri_all_data_YYYYMMDD_HHMMSS.csv` - Complete ESRI data export
- `esri_edc_schools_analysis_YYYYMMDD_HHMMSS.csv` - EDC schools coverage analysis

## Common Use Cases

### Daily Processing
```bash
# Run analysis to see current status
python 03_esri/01_esri_analysis.py

# Process any schools needing updates
python 03_esri/02_esri_processing.py
```

### Bulk Refresh
```bash
# Force refresh all EDC schools
python 03_esri/02_esri_processing.py --force-refresh
```

### Testing/Development
```bash
# Test with limited schools
python 03_esri/02_esri_processing.py --limit 5 --force-refresh
```

## Monitoring and Debugging

### Check Processing Status
```bash
python 03_esri/01_esri_analysis.py
```

### View Logs
The processor uses structured logging. For debug mode, set:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Common Issues

1. **Cloud SQL Proxy not found**: Install with `cloud_components install cloud_sql_proxy`
2. **Service account issues**: Ensure `../etl-service-account-key.json` exists and has proper permissions
3. **ESRI authentication**: Check `.env` file and credentials
4. **EDC schools file missing**: Ensure `../edc_schools/firebase_schools_06152025.csv` exists
5. **Location not found**: School may not have location_points data

## Example Queries

After processing, you can query the data:

```sql
-- Get all demographic data for EDC schools
SELECT ed.*, sl.school_id 
FROM esri_demographic_data ed
JOIN school_locations sl ON ed.location_id = sl.location_id
WHERE sl.school_id IN (SELECT school_id FROM edc_schools_list);

-- Compare drive times for a school
SELECT sl.school_id, ed.drive_time, ed.per_hisp_child_20, ed.per_wht_child_20, ed.medhinc_cy 
FROM esri_demographic_data ed
JOIN school_locations sl ON ed.location_id = sl.location_id
WHERE sl.school_id = 'your_school_id'
ORDER BY ed.drive_time;

-- Find EDC schools with high Hispanic child population
SELECT sl.school_id, ed.drive_time, ed.per_hisp_child_20
FROM esri_demographic_data ed
JOIN school_locations sl ON ed.location_id = sl.location_id
WHERE ed.per_hisp_child_20 > 0.5 
  AND sl.school_id IN (SELECT school_id FROM edc_schools_list)
ORDER BY ed.per_hisp_child_20 DESC;

-- Check processing status for EDC schools
SELECT ps.school_id, ps.esri_processed, ps.last_processed_at
FROM processing_status ps
WHERE ps.school_id IN (SELECT school_id FROM edc_schools_list)
  AND ps.esri_processed = true;
``` 