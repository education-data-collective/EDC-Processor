# ESRI Unified Processor

This module integrates the ESRI demographic data processing with the EDC Unified Database. It fetches coordinates from the `location_points` table, processes ESRI demographic data, and stores results in the `esri_demographic_data` table.

## Files Overview

- **`fetch.py`** - Original ESRI data fetching functionality from ArcGIS
- **`process.py`** - Original school-based processing logic
- **`unified_processor.py`** - New unified processor for EDC database integration
- **`cli.py`** - Command-line interface for easy processing
- **`example_usage.py`** - Example usage scripts

## Quick Start

### 1. Prerequisites

Make sure you have:
- Cloud SQL Proxy installed (`cloud-sql-proxy` or `cloud_sql_proxy`)
- Service account key file: `./etl-service-account-key.json`
- Python dependencies: `sqlalchemy`, `psycopg2`, `arcgis`, `shapely`
- ESRI credentials in `.env` file

### 2. Install Dependencies

```bash
pip install sqlalchemy psycopg2-binary arcgis shapely python-dotenv
```

### 3. Set up ESRI Credentials

Create a `.env` file in the project root:
```bash
ESRI_USERNAME=your_username_or_secret_ref
ESRI_PASSWORD=your_password_or_secret_ref
ESRI_URL=https://www.arcgis.com
```

### 4. Basic Usage

#### List Available Locations
```bash
python esri/cli.py list-locations
```

#### Process a Single Location
```bash
python esri/cli.py process-single --location-id 1
```

#### Process Multiple Locations
```bash
python esri/cli.py process-multiple --location-ids "1,2,3,4,5"
```

#### Check Existing Data
```bash
python esri/cli.py check-data
```

#### Force Refresh Data
```bash
python esri/cli.py process-single --location-id 1 --force-refresh
```

## Database Schema

The processor creates an `esri_demographic_data` table with the following structure:

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

## Python API Usage

```python
from esri.unified_processor import (
    start_cloud_sql_proxy, 
    stop_cloud_sql_proxy, 
    create_connection,
    process_location
)

# Start proxy and connect
proxy_process, port = start_cloud_sql_proxy()
engine = create_connection(port)

try:
    # Process a location
    success = process_location(engine, location_id=1, force_refresh=True)
    if success:
        print("Processing completed successfully!")
finally:
    # Cleanup
    stop_cloud_sql_proxy(proxy_process)
```

## Data Processing Flow

1. **Coordinate Lookup**: Fetches lat/lon from `location_points` table
2. **ESRI API Call**: Uses ArcGIS to get demographic data for 5/10/15 minute drive times
3. **Data Transformation**: Calculates percentages and validates data
4. **Database Storage**: Saves to `esri_demographic_data` table with proper constraints
5. **Caching**: Avoids re-processing recent data (within 30 days)

## Error Handling

The processor includes comprehensive error handling:
- Database connection failures
- ESRI API errors
- Data validation issues
- Transaction rollbacks on failures
- Detailed logging throughout the process

## Performance Considerations

- **Caching**: Recent data (within 30 days) is not re-processed unless `force_refresh=True`
- **Rate Limiting**: 2-second delays between API calls in batch processing
- **Transactions**: Atomic operations ensure data consistency
- **Indexing**: Database indexes on location_id, drive_time, and processed_at

## Monitoring and Debugging

### Check Processing Status
```bash
python esri/cli.py check-data
```

### View Logs
The processor uses structured logging. Set log level:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Common Issues

1. **Cloud SQL Proxy not found**: Install with `gcloud components install cloud_sql_proxy`
2. **Service account issues**: Ensure `./etl-service-account-key.json` exists and has proper permissions
3. **ESRI authentication**: Check `.env` file and credentials
4. **Location not found**: Use `list-locations` to see available location_ids

## Example Queries

After processing, you can query the data:

```sql
-- Get all demographic data for a location
SELECT * FROM esri_demographic_data WHERE location_id = 1;

-- Compare drive times for a location
SELECT drive_time, per_hisp_child_20, per_wht_child_20, medhinc_cy 
FROM esri_demographic_data 
WHERE location_id = 1 
ORDER BY drive_time;

-- Find locations with high Hispanic child population
SELECT location_id, drive_time, per_hisp_child_20
FROM esri_demographic_data 
WHERE per_hisp_child_20 > 0.5 
ORDER BY per_hisp_child_20 DESC;

-- Get recent processing summary
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT location_id) as unique_locations,
       MAX(processed_at) as last_update
FROM esri_demographic_data;
```

## Integration with Original ESRI Module

This unified processor is designed to work alongside the original ESRI processing for schools:

- **Unified Processor**: For general location-based demographic analysis
- **Original Process**: School-specific analysis with polygon relationships

Both can coexist and share the same ESRI credentials and data fetching logic. 