from .data_fetcher import fetch_historical_data, fetch_school_info
from .main import generate_and_update_projections

def initialize_projections(app):
    # You can add any initialization logic here if needed
    app.logger.info("Initializing enrollment projections")
    # For example, you might want to set some configuration values:
    app.config['ENROLLMENT_PROJECTIONS_INITIALIZED'] = True

# Export the initialize_projections function
__all__ = ['fetch_historical_data', 'fetch_school_info', 'generate_and_update_projections', 'initialize_projections']