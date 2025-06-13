# Testing Guide for EDC Processor

This project uses a **hybrid testing approach** with both unit tests (mocked) and integration tests (real services).

## 🧪 Testing Strategy

### **Unit Tests** (Fast, Isolated)
- **Purpose**: Test individual functions and components in isolation
- **Dependencies**: Mocked (Firebase, Database, External APIs)
- **Speed**: Very fast (~1-2 seconds)
- **Use Case**: Development, CI/CD, quick validation

### **Integration Tests** (Realistic, Slower)  
- **Purpose**: Test real integrations with Cloud SQL and Firebase
- **Dependencies**: Real test database and Firebase project
- **Speed**: Slower (~30-60 seconds)
- **Use Case**: Pre-deployment validation, catching integration issues

## 🚀 Quick Start

### Run Unit Tests (No Setup Required)
```bash
make test-unit
# or
pytest -m "unit" -v
```

### Run Integration Tests (Requires Setup)
```bash
make test-integration  
# or
pytest -m "integration" -v
```

## ⚙️ Environment Setup

### 1. Copy Environment Template
```bash
cp .env.example .env
```

### 2. Configure for Unit Tests (Minimal)
```env
FLASK_ENV=testing
SECRET_KEY=test-secret-key
DATABASE_URL=sqlite:///test.db
```

### 3. Configure for Integration Tests (Full Setup)

#### Database Configuration
```env
# Your Cloud SQL test database
TEST_DATABASE_URL=postgresql://username:password@your-test-cloud-sql-ip:5432/edc_test
```

#### Firebase Configuration
```env
# Your development Firebase project
TEST_FIREBASE_PROJECT_ID=your-test-project-id
TEST_FIREBASE_SERVICE_ACCOUNT_PATH=/path/to/test-service-account-key.json
```

Or use individual Firebase keys:
```env
FIREBASE_PROJECT_ID=your-test-project-id
FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nyour-key\n-----END PRIVATE KEY-----\n"
FIREBASE_CLIENT_EMAIL=test-service@your-project.iam.gserviceaccount.com
# ... other Firebase credentials
```

## 🗄️ Database Setup

### Cloud SQL Test Database Setup

1. **Create Test Database Instance**
   ```sql
   CREATE DATABASE edc_test;
   ```

2. **Run Migrations** (if you have them)
   ```bash
   # Set TEST_DATABASE_URL in .env first
   flask db upgrade
   ```

3. **Verify Connection**
   ```bash
   make test-integration-db
   ```

### Database Best Practices
- Use a **separate test database** - never test against production
- Tests automatically create and clean up test data
- Each test runs in a transaction that rolls back after completion

## 🔥 Firebase Setup

### Development Project Setup

1. **Create Firebase Test Project**
   - Go to [Firebase Console](https://console.firebase.google.com/)
   - Create a new project for testing (e.g., `your-project-test`)

2. **Generate Service Account Key**
   ```bash
   # Go to Project Settings > Service Accounts
   # Generate new private key and download JSON file
   ```

3. **Set Permissions**
   - Ensure service account has Firestore read/write permissions

4. **Verify Connection**
   ```bash
   make test-integration-firebase
   ```

### Firebase Best Practices
- Use a **dedicated test project** - separate from development/production
- Tests automatically clean up test documents
- Real-time features are tested with actual Firebase streams

## 📋 Test Commands

### Available Commands

| Command | Description | Speed | Dependencies |
|---------|-------------|-------|--------------|
| `make test-fast` | Unit tests only, minimal output | ⚡ Fast | None |
| `make test-unit` | Unit tests with verbose output | ⚡ Fast | None |
| `make test-integration` | All integration tests | 🐌 Slower | DB + Firebase |
| `make test-integration-db` | Database tests only | 🐌 Medium | Cloud SQL |
| `make test-integration-firebase` | Firebase tests only | 🐌 Medium | Firebase |
| `make test` | All tests (unit + integration) | 🐌 Slower | All services |
| `make test-cov` | All tests with coverage report | 🐌 Slower | All services |

### Pytest Markers

```bash
# Run only unit tests
pytest -m "unit"

# Run only integration tests  
pytest -m "integration"

# Run specific test file
pytest tests/unit/test_utils.py -v

# Run specific test method
pytest tests/integration/test_database_integration.py::TestDatabaseSchema::test_tables_exist -v
```

## 🔧 Development Workflow

### Daily Development
```bash
# Quick validation during development
make test-fast

# Before committing
make test-unit
```

### Before Deployment
```bash
# Full test suite
make test-integration

# With coverage report
make test-cov
```

### Debugging Integration Issues
```bash
# Test specific integration
make test-integration-db
make test-integration-firebase

# Detailed error output
pytest tests/integration/ -v --tb=long
```

## 🚨 Troubleshooting

### Unit Tests Failing
- Check import paths and dependencies
- Ensure mocks are properly configured
- Run `make clean` to clear cache

### Integration Tests Failing

#### Database Issues
- Verify `TEST_DATABASE_URL` is correct
- Check database connectivity: `psql $TEST_DATABASE_URL`
- Ensure test database exists and is accessible
- Check database permissions

#### Firebase Issues  
- Verify `TEST_FIREBASE_PROJECT_ID` and service account path
- Check service account permissions in Firebase Console
- Ensure Firestore is enabled in the test project
- Test Firebase connection independently

#### Network Issues
- Integration tests require internet connectivity
- Check firewall settings for Cloud SQL and Firebase
- Consider VPN issues if connecting to private resources

### Common Error Solutions

```bash
# Clear test cache
make clean
pytest --cache-clear

# Reinstall dependencies
pip install -r requirements-dev.txt

# Check environment variables
python -c "import os; print(os.getenv('TEST_DATABASE_URL'))"
```

## 📊 Coverage Reports

Coverage reports are generated in `htmlcov/index.html`:

```bash
make test-cov
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

**Target Coverage**: 80% minimum for production code

## 🎯 Testing Best Practices

### Unit Tests
- Test one function/method at a time
- Use mocks for all external dependencies
- Keep tests fast and deterministic
- Test edge cases and error conditions

### Integration Tests
- Test real workflows end-to-end
- Use test data that gets cleaned up
- Test actual database constraints and relationships
- Verify Firebase real-time features

### General Guidelines
- Write tests before fixing bugs
- Keep test data isolated (use test- prefixes)
- Clean up after tests (automatic with our fixtures)
- Use descriptive test names and docstrings

## 🔄 CI/CD Integration

### GitHub Actions
The pipeline runs both test types:

```yaml
# Unit tests run on every PR (fast feedback)
- name: Run Unit Tests
  run: make test-unit

# Integration tests run on develop/main (comprehensive validation)  
- name: Run Integration Tests
  run: make test-integration
  env:
    TEST_DATABASE_URL: ${{ secrets.TEST_DATABASE_URL }}
    TEST_FIREBASE_PROJECT_ID: ${{ secrets.TEST_FIREBASE_PROJECT_ID }}
```

### Local Pre-commit
```bash
# Runs unit tests only (keeps it fast)
make pre-commit
```

## 📚 Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [Cloud SQL Documentation](https://cloud.google.com/sql/docs)
- [Firebase Admin SDK](https://firebase.google.com/docs/admin/setup)
- [SQLAlchemy Testing](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites) 