# EDC Processor

[![CI/CD Pipeline](https://github.com/education-data-collective/EDC-Processor/actions/workflows/ci.yml/badge.svg)](https://github.com/education-data-collective/EDC-Processor/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/education-data-collective/EDC-Processor/branch/main/graph/badge.svg)](https://codecov.io/gh/education-data-collective/EDC-Processor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Modern entity processing system for schools and location points with unified schema support, designed for Cloud Run deployment.

## Overview

The EDC Processor is a modular, scalable processing system that handles automated workflows for educational data entities. It supports both schools and location points with flexible processing pipelines based on entity type and available data.

## Key Features

- ✅ **Unified Schema Support**: Single database with evolved schema design
- ✅ **Flexible Processing**: Different stages based on entity type and data availability
- ✅ **Queue-Based Processing**: Async task management with priority queues
- ✅ **Firebase Integration**: Real-time status tracking and progress monitoring
- ✅ **Modular Architecture**: Independent task handlers for each processing stage
- ✅ **Cloud Run Ready**: Optimized for Google Cloud Run deployment
- ✅ **Comprehensive API**: RESTful endpoints for processing, validation, and monitoring

## Architecture

```
EDC_PROCESSOR/
├── entity_processing/          # Modern processing system (v2)
│   ├── processor.py           # Core processing orchestrator
│   ├── task_manager.py        # Queue-based task management
│   ├── task_handlers/         # Individual processing stages
│   └── routes/               # API endpoints
├── models/                   # Unified database schema
├── unified_processing/       # Original processing system (legacy)
└── tests/                   # Test suite
```

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL
- Firebase project (for status tracking)
- Google Cloud SDK (for deployment)

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/education-data-collective/EDC-Processor.git
   cd EDC-Processor
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   make install-dev
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run database migrations**
   ```bash
   make db-upgrade
   ```

6. **Start development server**
   ```bash
   make run
   ```

## Processing Stages

### For Schools
1. **Location**: Validate and geocode school addresses
2. **Demographics**: Collect ESRI demographic data for location
3. **Enrollment**: Process enrollment data (if available)
4. **Projections**: Generate enrollment projections (if enrollment exists)
5. **Metrics**: Calculate district-level metrics

### For Location Points
1. **Demographics**: Collect ESRI demographic data for coordinates

## API Usage

### Process a Single Entity
```bash
curl -X POST http://localhost:5000/entity/process \
  -H "Content-Type: application/json" \
  -d '{"entity_id": 123, "entity_type": "school"}'
```

### Bulk Processing
```bash
curl -X POST http://localhost:5000/entity/process/bulk \
  -H "Content-Type: application/json" \
  -d '{"entities": [123, 124, 125], "entity_type": "school"}'
```

### Check Processing Status
```bash
curl -X GET http://localhost:5000/entity/status/123?entity_type=school
```

### Validate Entities
```bash
curl -X POST http://localhost:5000/entity/validate \
  -H "Content-Type: application/json" \
  -d '{"entity_id": 123, "entity_type": "school"}'
```

## Development Commands

```bash
# Install dependencies
make install-dev

# Run tests
make test
make test-cov

# Code formatting and linting
make format
make lint

# Security checks
make security

# Run development server
make run

# Build package
make build

# Clean up
make clean
```

## Testing

Run the full test suite:
```bash
make test-cov
```

Run specific test types:
```bash
make test-unit       # Unit tests only
make test-integration # Integration tests only
```

## Deployment

### Cloud Run

1. **Build Docker image**
   ```bash
   make docker-build
   ```

2. **Deploy to Cloud Run**
   ```bash
   gcloud run deploy edc-processor \
     --image gcr.io/your-project/edc-processor \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated
   ```

### Environment Variables

Required environment variables for deployment:

- `DATABASE_URL`: PostgreSQL connection string
- `FIREBASE_SERVICE_ACCOUNT`: Firebase service account JSON
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to Google Cloud credentials
- `FLASK_ENV`: Application environment (production/development)

## Contributing

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Make your changes**
4. **Run tests** (`make test`)
5. **Format code** (`make format`)
6. **Commit changes** (`git commit -m 'Add amazing feature'`)
7. **Push to branch** (`git push origin feature/amazing-feature`)
8. **Open a Pull Request**

## Development Workflow

### Branch Strategy
- `main`: Production-ready code
- `develop`: Integration branch for features
- `feature/*`: Individual feature branches

### Code Quality
- All code must pass CI/CD pipeline
- Minimum 80% test coverage required
- Code formatting with Black and isort
- Type hints with mypy
- Security scanning with bandit

## Documentation

- [Entity Processing v2 Documentation](entity_processing/README.md)
- [API Documentation](docs/api.md) (coming soon)
- [Deployment Guide](docs/deployment.md) (coming soon)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions or support:
- Open an issue on GitHub
- Contact: contact@educationdatacollective.com

## Changelog

### v2.0.0 (Current)
- New entity processing system with unified schema
- Support for both schools and location points
- Queue-based processing with async task management
- Firebase integration for real-time status tracking
- Cloud Run ready architecture

### v1.0.0 (Legacy)
- Original unified processing system
- School-focused processing pipeline 