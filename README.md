# EDC Processor - Database Test

This is a simple database connection test for the EDC Processor project.

## Prerequisites

1. **Cloud SQL Proxy**: Install the Google Cloud SQL Proxy
   ```bash
   # On macOS
   brew install cloud-sql-proxy
   
   # Or download directly
   curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.8.0/cloud-sql-proxy.darwin.amd64
   chmod +x cloud-sql-proxy
   ```

2. **Service Account Key**: Place your `etl-service-account-key.json` file in the project root directory

3. **Python Dependencies**: Install required packages
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the database test script:

```bash
python database_test.py
```

## What the Test Does

The script will:

1. **Start Cloud SQL Proxy** - Establishes a secure connection to your Cloud SQL database
2. **Test Connection** - Verifies that the database connection works
3. **Explore Database Structure** - Lists all tables in the database
4. **Check Data** - Shows row counts for each table and column names
5. **Sample Data** - Displays sample records from tables that contain data

## Configuration

Update the configuration section in `database_test.py` if needed:

```python
PROJECT_ID = 'enrollment-risk-v2'
CLOUD_SQL_CONNECTION_NAME = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
SERVICE_ACCOUNT_FILE = './etl-service-account-key.json'
DB_NAME = 'edc_unified'
DB_USER = 'admin'
DB_PASSWORD = 'edc4thew!n'
```

## Expected Output

The script will show:
- Cloud SQL Proxy startup status
- Database connection status
- List of all tables in the database
- Record counts for each table
- Column names for tables with data
- Sample data from populated tables

## Troubleshooting

- **Cloud SQL Proxy not found**: Install the proxy using the instructions above
- **Service account file missing**: Ensure `etl-service-account-key.json` is in the project root
- **Connection failed**: Check your database credentials and network connectivity
- **Permission denied**: Verify your service account has the necessary Cloud SQL permissions 