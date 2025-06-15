# Nearby Schools Processor

This module processes ESRI drive-time polygons to identify schools within catchment areas and populates the `school_polygon_relationships` and `nearby_school_polygons` tables in the EDC Unified Database.

## Overview

The Nearby Schools Processor uses ESRI demographic data (specifically drive-time polygons) to find schools that fall within specific geographic catchment areas around target locations. This enables spatial analysis for understanding school accessibility and competition.

## Files

- **`processor.py`** - Main processing engine
- **`cli.py`** - Command-line interface
- **`README.md`** - This documentation

## Prerequisites

1. **ESRI Data**: Must have ESRI demographic data processed first (from the `esri/` module)
2. **School Location Data**: Schools must be linked to locations via the `school_locations` table
3. **Dependencies**: `sqlalchemy`, `psycopg2-binary`, `shapely`

## Quick Start

### 1. Check Available Data

First, see what ESRI polygon data is available:
```bash
python nearby_schools/cli.py locations-with-esri
```

Check what schools have locations for a given year:
```bash
python nearby_schools/cli.py list-school-locations --data-year 2024
```

### 2. Process a Single Location

```bash
python nearby_schools/cli.py process-location --location-id 1 --data-year 2024
```

### 3. Process Multiple Locations

```bash
python nearby_schools/cli.py process-multiple --location-ids "1,2,3,4,5" --data-year 2024
```

### 4. Check Results

```bash
python nearby_schools/cli.py check-data --data-year 2024
```

## How It Works

### 1. Data Input
- **Target Location**: Gets ESRI drive-time polygons from `esri_demographic_data` table
- **School Locations**: Gets all schools with coordinates from `school_locations` + `location_points` tables

### 2. Spatial Analysis
- Uses Shapely geometry library to parse ESRI polygon JSON
- Creates `Point` objects for each school location
- Tests if each school point falls within drive-time polygons (5, 10, 15 minutes)

### 3. Data Storage
- **`school_polygon_relationships`**: Records the polygon analysis for each location/drive time combination
- **`nearby_school_polygons`**: Records which specific schools fall within each polygon

## Database Schema

### school_polygon_relationships
```sql
CREATE TABLE school_polygon_relationships (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES location_points(id),
    drive_time INTEGER NOT NULL,
    data_year INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(location_id, drive_time, data_year)
);
```

### nearby_school_polygons
```sql
CREATE TABLE nearby_school_polygons (
    id SERIAL PRIMARY KEY,
    polygon_relationship_id INTEGER NOT NULL REFERENCES school_polygon_relationships(id),
    school_uuid VARCHAR(36) NOT NULL,
    relationship_type VARCHAR(20) NOT NULL DEFAULT 'nearby',
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(polygon_relationship_id, school_uuid, relationship_type)
);
```

## CLI Commands

### process-location
Process nearby schools for a single location:
```bash
python nearby_schools/cli.py process-location --location-id 1 --data-year 2024 [--force-refresh]
```

### process-multiple
Process nearby schools for multiple locations:
```bash
python nearby_schools/cli.py process-multiple --location-ids "1,2,3,4,5" --data-year 2024 [--force-refresh]
```

### check-data
View processing summary and statistics:
```bash
python nearby_schools/cli.py check-data --data-year 2024
```

### list-school-locations
List schools that have location data:
```bash
python nearby_schools/cli.py list-school-locations --data-year 2024
```

### locations-with-esri
List locations that have ESRI polygon data available:
```bash
python nearby_schools/cli.py locations-with-esri
```

## Python API

```python
from nearby_schools.processor import (
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
    success = process_location(engine, location_id=1, data_year=2024, force_refresh=True)
    if success:
        print("Processing completed successfully!")
finally:
    # Cleanup
    stop_cloud_sql_proxy(proxy_process)
```

## Data Flow

1. **Input Validation**: Check if location has ESRI polygons and schools exist for the data year
2. **Polygon Retrieval**: Fetch drive-time polygons (5, 10, 15 minutes) from ESRI data
3. **School Retrieval**: Get all schools with valid coordinates for the data year
4. **Spatial Analysis**: For each drive-time polygon:
   - Parse ESRI JSON to create Shapely polygon
   - Test each school location point for containment
   - Collect schools within each polygon
5. **Data Storage**: Store polygon relationships and nearby school mappings

## Error Handling

- **Missing ESRI Data**: If no polygons exist for a location, processing fails with clear error
- **Invalid Polygons**: Malformed polygon JSON is skipped with warnings
- **No Schools**: If no schools exist for the data year, processing fails
- **Database Errors**: Transaction rollback ensures data consistency

## Performance Considerations

- **Caching**: Existing data is not re-processed unless `force_refresh=True`
- **Batch Processing**: Multiple locations can be processed efficiently
- **Spatial Optimization**: Uses Shapely's optimized geometric operations
- **Transaction Safety**: All operations are wrapped in database transactions

## Example Queries

After processing, you can query the relationships:

```sql
-- Get nearby schools for a specific location and drive time
SELECT nsp.school_uuid, s.id as school_id
FROM school_polygon_relationships spr
JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
JOIN schools s ON nsp.school_uuid = s.uuid
WHERE spr.location_id = 1 
  AND spr.drive_time = 10
  AND spr.data_year = 2024;

-- Count nearby schools by drive time for a location
SELECT 
    spr.drive_time,
    COUNT(nsp.school_uuid) as nearby_school_count
FROM school_polygon_relationships spr
LEFT JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
WHERE spr.location_id = 1 AND spr.data_year = 2024
GROUP BY spr.drive_time
ORDER BY spr.drive_time;

-- Find locations with the most nearby schools (15-minute drive time)
SELECT 
    spr.location_id,
    COUNT(nsp.school_uuid) as nearby_school_count
FROM school_polygon_relationships spr
JOIN nearby_school_polygons nsp ON spr.id = nsp.polygon_relationship_id
WHERE spr.drive_time = 15 AND spr.data_year = 2024
GROUP BY spr.location_id
ORDER BY nearby_school_count DESC
LIMIT 10;
```

## Integration with ESRI Module

This module depends on the ESRI demographic processor:

1. **Run ESRI processing first** to generate drive-time polygons
2. **Then run nearby schools processing** to analyze school relationships
3. **Use both datasets together** for comprehensive demographic and competitive analysis

## Troubleshooting

### No ESRI Polygons Found
```bash
# Check if ESRI data exists
python nearby_schools/cli.py locations-with-esri

# If no data, process ESRI first
python esri/cli.py process-single --location-id 1
```

### No Schools Found
```bash
# Check available school locations
python nearby_schools/cli.py list-school-locations --data-year 2024

# Verify school_locations table has data for your target year
```

### Processing Errors
- Check Cloud SQL Proxy is running
- Verify service account permissions
- Ensure database connections are working
- Check log output for specific error details 