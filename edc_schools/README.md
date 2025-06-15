# Firebase Schools Enrichment Report

This directory contains tools for creating enriched reports from Firebase school data by combining it with address information from the EDC database.

## Scripts

### `firebase_schools_enrichment.py`

A comprehensive script that:
1. Loads the Firebase schools CSV file
2. Progressively pulls address data from the EDC database in batches
3. Enriches the Firebase data with complete address information
4. Generates a detailed report with statistics
5. Saves the enriched data as a new CSV file

## Usage

### Basic Usage

```bash
python firebase_schools_enrichment.py edc_schools/firebase_schools_06152025.csv
```

### Advanced Usage

```bash
python firebase_schools_enrichment.py edc_schools/firebase_schools_06152025.csv \
  --output edc_schools/reports/enriched_schools_$(date +%Y%m%d).csv \
  --verbose \
  --batch-size 50
```

### Command Line Options

- `csv_file`: Path to the Firebase schools CSV file (required)
- `-o, --output`: Output CSV file path (default: `edc_schools/reports/firebase_schools_enriched.csv`)
- `-v, --verbose`: Enable verbose logging for debugging
- `-b, --batch-size`: Number of schools to process per batch (default: 100)

## Output

The script generates two files:
1. **Enriched CSV**: Contains all original Firebase columns plus new address columns
2. **Statistics file**: Summary of processing results and coverage metrics

### New Address Columns Added

- `address`: Street address
- `city`: City name  
- `state`: State abbreviation
- `county`: County name
- `zipcode`: ZIP code
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `database_school_name`: School name from database
- `database_state_name`: Full state name from database
- `lea_name`: Local Education Agency name
- `school_status`: Current school status
- `data_year`: Data year from database
- `school_year`: School year format
- `match_type`: How the school was matched (direct, suffix_mapped, no_match)
- `has_address_data`: Boolean indicating if address data was found

## Database Connection

The script automatically:
- Starts a Cloud SQL Proxy connection
- Connects to the EDC database
- Handles both direct NCES ID matches and suffix-mapped IDs
- Gracefully handles connection errors
- Cleans up resources on exit

## Requirements

- Python 3.7+
- pandas
- sqlalchemy
- Cloud SQL Proxy installed
- EDC service account key file (`etl-service-account-key.json` in parent directory)

## Features

- **Progressive Processing**: Processes schools in configurable batches to avoid overwhelming the database
- **Comprehensive Matching**: Handles both direct NCES ID matches and hyphenated suffix mappings
- **Error Handling**: Continues processing even if some database queries fail
- **Rich Reporting**: Provides detailed statistics and coverage metrics
- **Resource Management**: Automatically cleans up database connections and proxy processes
- **Signal Handling**: Gracefully handles interruption signals (Ctrl+C)

## Example Output

```
=====================================
FIREBASE SCHOOLS ENRICHMENT REPORT
=====================================

📊 PROCESSING SUMMARY
Total CSV records loaded: 2,683
Unique schools processed: 1,244
Batches processed: 13
Database connection errors: 0

Address Data Enrichment Results
-------------------------------------
Schools with addresses: 1,156
Schools without addresses: 88
Address coverage: 92.9%

✅ GOOD ADDRESS COVERAGE 