# Geocoding Scripts

This directory contains scripts for geocoding location_points in the EDC database using the Google Maps API.

## Scripts

### 01_geocode_analysis.py
Analyzes the location_points table to identify which records need geocoding (missing coordinates and/or county data).

**Usage:**
```bash
# Run analysis
python 01_geocode_analysis.py

# Run analysis and export results to CSV
python 01_geocode_analysis.py --export
```

**Features:**
- Identifies locations missing coordinates
- Identifies locations missing county data
- Identifies locations with no geocodable data
- Estimates API usage and costs
- Exports results to CSV files for processing

### 02_geocode_process.py
Processes location_points using the Google Maps API to fill in missing coordinates and county data.

**Usage:**
```bash
# Process coordinate geocoding only
python 02_geocode_process.py --coordinates

# Process county geocoding only
python 02_geocode_process.py --county

# Process both coordinates and county data
python 02_geocode_process.py --all

# Dry run (show what would be done without making changes)
python 02_geocode_process.py --all --dry-run

# Process with limits (for testing)
python 02_geocode_process.py --coordinates --limit 10

# Validate geocoding results
python 02_geocode_process.py --validate
```

**Features:**
- Rate-limited API calls to respect Google Maps limits
- Batch processing with progress tracking
- Comprehensive logging
- Validation of results
- Dry-run mode for testing
- Handles both forward and reverse geocoding

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r 01_geocode/requirements.txt
   ```

2. **Environment variables:**
   Make sure your `.env` file in the project root contains:
   ```
   GOOGLE_MAPS_API_KEY=your_api_key_here
   ```

3. **Service account:**
   Ensure `etl-service-account-key.json` is in the project root for database access.

## Workflow

1. **Analysis first:**
   ```bash
   python 01_geocode_analysis.py --export
   ```
   This will create CSV files with locations needing geocoding.

2. **Test with dry run:**
   ```bash
   python 02_geocode_process.py --all --dry-run --limit 5
   ```

3. **Process coordinates:**
   ```bash
   python 02_geocode_process.py --coordinates
   ```

4. **Process county data:**
   ```bash
   python 02_geocode_process.py --county
   ```

5. **Validate results:**
   ```bash
   python 02_geocode_process.py --validate
   ```

## API Usage and Costs

- Google Maps Geocoding API: ~$5 per 1,000 requests
- Rate limited to 50 requests per second
- The analysis script estimates costs before processing
- Monitor usage in Google Cloud Console

## Logging

- Detailed logs are saved to `01_geocode/logs/`
- Both console and file logging
- Includes API responses and errors

## Database Updates

The processing script updates the following fields in `location_points`:
- `latitude` and `longitude` from geocoding
- `county` from address components or reverse geocoding
- `updated_at` timestamp

## Error Handling

- Graceful handling of API errors
- Rate limiting with delays
- Database transaction safety
- Signal handling for clean shutdown 