# EDC Schools Scripts

This folder contains scripts for processing and enriching EDC school data.

## `firebase_schools_enrichment.py`

Enriches the `edc_schools.csv` file with complete address information from the EDC database.

### Usage

**Basic (uses defaults):**
```bash
python firebase_schools_enrichment.py
```

**With custom file:**
```bash
python firebase_schools_enrichment.py path/to/your/schools.csv
```

**With options:**
```bash
python firebase_schools_enrichment.py ../firebase_data/edc_schools.csv --verbose --batch-size 50
```

### Default Behavior

- **Input**: `../firebase_data/edc_schools.csv`
- **Output**: `../reports/edc_schools_enriched_YYYYMMDD_HHMMSS.csv`
- **Batch size**: 100 schools per database query

### Output Columns

The script adds these address columns to your original data:
- `address`, `city`, `state`, `county`, `zipcode`
- `latitude`, `longitude`
- `database_school_name`, `lea_name`, `school_status`
- `match_type`, `has_address_data`

### Requirements

- Cloud SQL Proxy installed
- Service account key file in project root
- Python packages: pandas, sqlalchemy 