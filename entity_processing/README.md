# Entity Processing v2

Modern processing system for schools and location points using the unified schema. This system replaces the original `unified_processing` module with a more flexible, scalable architecture.

## Overview

The Entity Processing system handles automated processing workflows for:
- **Schools**: Full processing pipeline including location validation, demographics, enrollment, projections, and metrics
- **Location Points**: Basic processing for geocoding and demographic data collection

## Key Features

- ✅ **Unified Schema Support**: Works with the new single-database models
- ✅ **Flexible Processing**: Different stages based on entity type and available data
- ✅ **Queue-Based Processing**: Async task management with priority queues
- ✅ **Firebase Integration**: Real-time status tracking and progress monitoring
- ✅ **Modular Architecture**: Independent task handlers for each processing stage
- ✅ **Cloud Run Ready**: Designed for deployment as a separate microservice

## Architecture

```
entity_processing/
├── __init__.py              # Main exports and blueprint
├── processor.py             # Core EntityProcessor class
├── task_manager.py          # Queue-based task management
├── utils.py                 # Utility functions
├── task_handlers/           # Individual processing stages
│   ├── __init__.py
│   ├── location.py          # Geocoding and location validation
│   ├── demographics.py      # ESRI demographic data collection
│   ├── enrollment.py        # School enrollment processing
│   ├── projections.py       # Enrollment projections
│   ├── metrics.py           # District metrics calculation
│   └── team.py              # Team assignment management
└── routes/                  # API endpoints
    ├── __init__.py
    ├── processing.py        # Main processing endpoints
    ├── validation.py        # Entity validation endpoints
    ├── status.py            # Status monitoring endpoints
    └── tasks.py             # Individual task endpoints
```

## Processing Stages

### For Schools
1. **Location**: Validate and geocode school address if needed
2. **Demographics**: Collect ESRI demographic data for location
3. **Enrollment**: Process enrollment data (if available)
4. **Projections**: Generate enrollment projections (if enrollment exists)
5. **Metrics**: Calculate district-level metrics

### For Location Points
1. **Demographics**: Collect ESRI demographic data for coordinates

## API Endpoints

### Main Processing
- `POST /process` - Process a single entity
- `POST /process/bulk` - Process multiple entities
- `POST /process/stage/{stage}` - Process a specific stage
- `POST /process/preview` - Preview processing stages without executing

### Validation
- `POST /validate` - Validate a single entity
- `POST /validate/bulk` - Validate multiple entities

### Status Monitoring
- `GET /status/{entity_id}` - Get processing status for an entity
- `POST /status/batch` - Get status for multiple entities
- `GET /status/processing` - Get currently processing entities
- `GET /status/summary` - Get overall processing statistics

### Individual Tasks
- `POST /tasks/location` - Run location processing task
- `POST /tasks/demographics` - Run demographics processing task
- `POST /tasks/enrollment` - Run enrollment processing task
- `POST /tasks/projections` - Run projections processing task
- `POST /tasks/metrics` - Run metrics processing task
- `POST /tasks/team` - Run team assignment task

## Usage Examples

### Process a Single School
```python
from entity_processing import EntityProcessor

# Create processor
processor = EntityProcessor(school_id=123, entity_type='school')

# Process all applicable stages
success, error = await processor.process()
```

### Process a Location Point
```python
# Process location point (demographics only)
processor = EntityProcessor(location_id=456, entity_type='location')
success, error = await processor.process()
```

### Using the Task Manager
```python
from entity_processing.task_manager import task_manager

# Start task manager
await task_manager.start()

# Add processing task
result = await task_manager.add_processing_task(
    entity_id=123,
    entity_type='school',
    priority='high'
)

# Add bulk processing
result = await task_manager.add_bulk_processing_task(
    entity_list=[123, 124, 125],
    entity_type='school'
)
```

### API Usage
```bash
# Process a school
curl -X POST http://localhost:5000/entity/process \
  -H "Content-Type: application/json" \
  -d '{"entity_id": 123, "entity_type": "school"}'

# Get processing status
curl -X GET http://localhost:5000/entity/status/123?entity_type=school

# Validate entities before processing
curl -X POST http://localhost:5000/entity/validate/bulk \
  -H "Content-Type: application/json" \
  -d '{"entities": [123, 124, 125], "entity_type": "school"}'
```

## Integration Notes

### Migrating from unified_processing

1. **Update Imports**: Replace `unified_processing` imports with `entity_processing`
2. **Entity Types**: Use `entity_type` parameter to specify 'school' or 'location'
3. **Task Handlers**: Individual task handlers are now more modular and reusable
4. **Status Tracking**: New Firebase-based status tracking provides better visibility

### Cloud Run Deployment

This system is designed for Cloud Run deployment:

1. **Environment Variables**: Set up database and Firebase credentials
2. **Async Support**: Use `asyncio` compatible WSGI server (e.g., `uvicorn` with `fastapi`)
3. **Scaling**: Configure CPU/memory limits based on workload
4. **Database Access**: Ensure Cloud Run can access your database

### Integration with Main App

```python
# In your main Flask app
from entity_processing import entity_bp

app.register_blueprint(entity_bp, url_prefix='/entity')
```

## Configuration

The system uses the same models and database configuration as your main application. Key requirements:

- **Models**: Uses the unified schema models from `/models`
- **Firebase**: Requires Firebase Admin SDK for status tracking
- **Database**: Standard SQLAlchemy connection via `app.db`

## Monitoring

- **Firebase**: Real-time processing status in `entity_processing` collection
- **Logs**: Comprehensive logging at all processing stages
- **Queue Status**: Monitor task queue sizes and worker status
- **API Health**: Health check endpoints for monitoring

## Future Enhancements

- [ ] **Retry Logic**: Automatic retry for failed processing stages
- [ ] **Webhooks**: Callback URLs for processing completion
- [ ] **Batch Optimization**: Smarter batching for bulk operations
- [ ] **Rate Limiting**: Configurable rate limits for external API calls
- [ ] **Metrics Dashboard**: Real-time processing metrics and analytics 