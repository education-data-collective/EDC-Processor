#!/usr/bin/env python3
"""
Firebase Schools Enrichment Script

This script takes the edc_schools.csv file and enriches it with address information
from the EDC database, progressively pulling data in batches to create a comprehensive report.
"""

import argparse
import sys
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import text, create_engine
from pathlib import Path
import os
import subprocess
import socket
import time
import signal
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

class MigrationLogger:
    """Simple logger implementation"""
    def __init__(self, name, verbose=False):
        self.logger = logging.getLogger(name)
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    def get_logger(self):
        return self.logger

def print_section_header(title, char="="):
    print(f"\n{char * len(title)}")
    print(title)
    print(f"{char * len(title)}")

def format_number(num):
    return f"{num:,}"

class LocalCloudSQLProxy:
    """Local Cloud SQL Proxy manager for database connection"""
    
    def __init__(self, connection_name: str, service_account_file: str, logger):
        self.connection_name = connection_name
        self.service_account_file = service_account_file
        self.logger = logger
        self.process = None
        self.port = None
    
    def _find_free_port(self) -> int:
        """Find a free port for the proxy"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]
    
    def start(self) -> int:
        """Start Cloud SQL proxy and return the port number"""
        try:
            # Find available port
            self.port = self._find_free_port()
            
            # Try different proxy command names
            proxy_commands = ['cloud-sql-proxy', 'cloud_sql_proxy']
            proxy_cmd = None
            
            for cmd in proxy_commands:
                try:
                    subprocess.run([cmd, '--version'], capture_output=True, check=True)
                    proxy_cmd = [
                        cmd,
                        f'-instances={self.connection_name}=tcp:{self.port}',
                        f'-credential_file={self.service_account_file}',
                        '-max_connections=50',
                    ]
                    break
                except (subprocess.CalledProcessError, FileNotFoundError):
                    continue
            
            if not proxy_cmd:
                raise Exception("Cloud SQL Proxy not found. Please install it first.")
            
            self.logger.info(f"Starting Cloud SQL Proxy on port {self.port}")
            
            # Start proxy process
            self.process = subprocess.Popen(
                proxy_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for startup
            time.sleep(5)
            
            # Check if process started successfully
            if self.process.poll() is not None:
                _, stderr = self.process.communicate()
                raise Exception(f"Cloud SQL Proxy failed to start: {stderr.decode()}")
            
            # Verify connection
            if not self._check_health():
                raise Exception("Cloud SQL Proxy health check failed")
            
            self.logger.info("✅ Cloud SQL Proxy started successfully")
            return self.port
            
        except Exception as e:
            self.logger.error(f"Failed to start Cloud SQL Proxy: {str(e)}")
            self.stop()
            raise
    
    def stop(self):
        """Stop Cloud SQL proxy"""
        if self.process:
            self.logger.info("Stopping Cloud SQL Proxy...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.logger.warning("Proxy didn't stop gracefully, killing...")
                self.process.kill()
                self.process.wait()
            
            self.process = None
            self.port = None
            self.logger.info("Cloud SQL Proxy stopped")
    
    def _check_health(self) -> bool:
        """Check if proxy is running and accessible"""
        if not self.process or self.process.poll() is not None:
            return False
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                result = s.connect_ex(('localhost', self.port))
                return result == 0
        except Exception:
            return False

class FirebaseSchoolsEnrichment:
    """Enriches Firebase schools data with address information from EDC database"""
    
    def __init__(self, csv_file_path: str, verbose: bool = False, batch_size: int = 100):
        self.logger_mgr = MigrationLogger("firebase_schools_enrichment", verbose)
        self.logger = self.logger_mgr.get_logger()
        self.csv_file_path = csv_file_path
        self.batch_size = batch_size
        self.firebase_schools = None
        self.enriched_data = []
        self.proxy = None
        self.edc_engine = None
        self.processing_stats = {
            'total_records': 0,
            'unique_schools': 0,
            'processed_batches': 0,
            'schools_with_addresses': 0,
            'schools_without_addresses': 0,
            'database_errors': 0
        }
        
        # Register signal handlers for cleanup
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, cleaning up...")
        self._cleanup()
        sys.exit(1)
        
    def _cleanup(self):
        """Clean up resources"""
        if self.proxy:
            self.proxy.stop()
        if self.edc_engine:
            self.edc_engine.dispose()
    
    def load_firebase_schools(self):
        """Load and prepare Firebase schools CSV"""
        self.logger.info("Loading Firebase schools CSV...")
        
        try:
            # Load CSV file
            self.firebase_schools = pd.read_csv(self.csv_file_path)
            self.processing_stats['total_records'] = len(self.firebase_schools)
            self.logger.info(f"Loaded {self.processing_stats['total_records']} records from CSV")
            
            # Get unique schools (deduplicate by school_id while preserving all other columns)
            self.firebase_schools = self.firebase_schools.drop_duplicates(subset=['school_id'])
            self.processing_stats['unique_schools'] = len(self.firebase_schools)
            self.logger.info(f"Found {self.processing_stats['unique_schools']} unique schools after deduplication")
            
            # Display column information
            self.logger.info(f"CSV columns: {', '.join(self.firebase_schools.columns.tolist())}")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV: {str(e)}")
            raise
    
    def _start_proxy(self):
        """Start Cloud SQL Proxy"""
        try:
            self.logger.info("Starting Cloud SQL Proxy...")
            
            # Configuration - service account is in project root
            connection_name = 'enrollment-risk-v2:us-central1:enrollment-risk-v2-dev-db'
            service_account_path = Path(__file__).parent.parent.parent / 'etl-service-account-key.json'
            
            if not service_account_path.exists():
                raise Exception(f"Service account file not found: {service_account_path}")
            
            self.proxy = LocalCloudSQLProxy(
                connection_name=connection_name,
                service_account_file=str(service_account_path),
                logger=self.logger
            )
            
            port = self.proxy.start()
            self.logger.info(f"✅ Cloud SQL Proxy started on port {port}")
            return port
            
        except Exception as e:
            self.logger.error(f"Failed to start Cloud SQL Proxy: {str(e)}")
            raise
    
    def connect_to_database(self):
        """Connect to the EDC database"""
        self.logger.info("Connecting to EDC database...")
        
        try:
            # Start Cloud SQL Proxy
            proxy_port = self._start_proxy()
            
            # Create database connection using proxy port
            db_url = f"postgresql://admin:edc4thew!n@localhost:{proxy_port}/edc_unified"
            self.edc_engine = create_engine(db_url)
            
            # Test connection
            with self.edc_engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM schools"))
                school_count = result.scalar()
                self.logger.info(f"✅ Connected to EDC database. Found {school_count} schools.")
            
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def get_address_data_for_schools(self, school_ids: List[str]) -> pd.DataFrame:
        """Get address data for a batch of school IDs from the database"""
        try:
            # Create placeholders for SQL IN clause
            placeholders = ','.join([f"'{school_id}'" for school_id in school_ids])
            
            address_query = f"""
            SELECT DISTINCT
                -- School identifier
                CASE 
                    WHEN sd.split_suffix IS NOT NULL THEN CONCAT(sd.ncessch, '-', sd.split_suffix)
                    ELSE sd.ncessch
                END as school_id,
                
                -- Address information from location_points
                lp.address,
                lp.city,
                lp.state,
                lp.county,
                lp.zip_code,
                
                -- Additional school information
                sd.system_name as database_school_name,
                sd.state_name as database_state_name,
                sd.lea_name,
                s.status as school_status,
                
                -- Location coordinates
                lp.latitude,
                lp.longitude,
                
                -- Data provenance
                sl.data_year,
                sl.school_year,
                sl.is_current,
                
                -- Match type for debugging
                'direct' as match_type
                
            FROM school_directory sd
            JOIN schools s ON sd.school_id = s.id
            LEFT JOIN school_locations sl ON s.id = sl.school_id AND sl.is_current = true
            LEFT JOIN location_points lp ON sl.location_id = lp.id
            WHERE sd.is_current = true
            AND (CASE 
                    WHEN sd.split_suffix IS NOT NULL THEN CONCAT(sd.ncessch, '-', sd.split_suffix)
                    ELSE sd.ncessch
                END) IN ({placeholders})
                
            UNION ALL
            
            -- Include schools from suffix mapping table (hyphenated IDs that map to base IDs)
            SELECT DISTINCT
                sm.hyphenated_ncessch as school_id,
                lp.address,
                lp.city,
                lp.state,
                lp.county,
                lp.zip_code,
                sd.system_name as database_school_name,
                sd.state_name as database_state_name,
                sd.lea_name,
                s.status as school_status,
                lp.latitude,
                lp.longitude,
                sl.data_year,
                sl.school_year,
                sl.is_current,
                'suffix_mapped' as match_type
                
            FROM temp_split_migration_suffix_mapping sm
            JOIN school_directory sd ON sd.ncessch = sm.base_ncessch AND sd.is_current = true
            JOIN schools s ON sd.school_id = s.id
            LEFT JOIN school_locations sl ON s.id = sl.school_id AND sl.is_current = true
            LEFT JOIN location_points lp ON sl.location_id = lp.id
            WHERE sm.hyphenated_ncessch IN ({placeholders})
            """
            
            with self.edc_engine.connect() as conn:
                address_df = pd.read_sql(address_query, conn)
            
            return address_df
            
        except Exception as e:
            self.logger.error(f"Error querying address data for batch: {str(e)}")
            self.processing_stats['database_errors'] += 1
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def process_schools_in_batches(self):
        """Process schools in batches to enrich with address data"""
        self.logger.info(f"Processing {self.processing_stats['unique_schools']} schools in batches of {self.batch_size}...")
        
        # Split schools into batches
        school_batches = []
        for i in range(0, len(self.firebase_schools), self.batch_size):
            batch = self.firebase_schools.iloc[i:i+self.batch_size]
            school_batches.append(batch)
        
        total_batches = len(school_batches)
        self.logger.info(f"Created {total_batches} batches for processing")
        
        # Process each batch
        for batch_num, batch_df in enumerate(school_batches, 1):
            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_df)} schools)")
            
            # Get school IDs for this batch
            school_ids = batch_df['school_id'].tolist()
            
            # Get address data from database
            address_data = self.get_address_data_for_schools(school_ids)
            
            # Merge Firebase data with address data
            enriched_batch = self._merge_batch_data(batch_df, address_data)
            
            # Add to enriched data collection
            self.enriched_data.extend(enriched_batch)
            
            # Update stats
            self.processing_stats['processed_batches'] += 1
            batch_with_addresses = sum(1 for record in enriched_batch if record.get('address'))
            batch_without_addresses = len(enriched_batch) - batch_with_addresses
            self.processing_stats['schools_with_addresses'] += batch_with_addresses
            self.processing_stats['schools_without_addresses'] += batch_without_addresses
            
            self.logger.info(f"Batch {batch_num} complete: {batch_with_addresses} with addresses, {batch_without_addresses} without")
            
            # Add small delay to avoid overwhelming the database
            time.sleep(0.1)
        
        self.logger.info("✅ All batches processed successfully")
    
    def _merge_batch_data(self, firebase_batch: pd.DataFrame, address_data: pd.DataFrame) -> List[Dict]:
        """Merge Firebase batch data with address data"""
        enriched_records = []
        
        for _, firebase_row in firebase_batch.iterrows():
            school_id = firebase_row['school_id']
            
            # Start with Firebase data
            enriched_record = firebase_row.to_dict()
            
            # Find matching address data
            address_matches = address_data[address_data['school_id'] == school_id]
            
            if not address_matches.empty:
                # Use the first match (there should typically be only one current record)
                address_row = address_matches.iloc[0]
                
                # Add address fields
                enriched_record.update({
                    'address': address_row.get('address', ''),
                    'city': address_row.get('city', ''),
                    'state': address_row.get('state', ''),
                    'county': address_row.get('county', ''),
                    'zipcode': address_row.get('zip_code', ''),
                    'latitude': address_row.get('latitude'),
                    'longitude': address_row.get('longitude'),
                    'database_school_name': address_row.get('database_school_name', ''),
                    'database_state_name': address_row.get('database_state_name', ''),
                    'lea_name': address_row.get('lea_name', ''),
                    'school_status': address_row.get('school_status', ''),
                    'data_year': address_row.get('data_year'),
                    'school_year': address_row.get('school_year', ''),
                    'match_type': address_row.get('match_type', ''),
                    'has_address_data': True
                })
            else:
                # No address data found
                enriched_record.update({
                    'address': '',
                    'city': '',
                    'state': '',
                    'county': '',
                    'zipcode': '',
                    'latitude': None,
                    'longitude': None,
                    'database_school_name': '',
                    'database_state_name': '',
                    'lea_name': '',
                    'school_status': '',
                    'data_year': None,
                    'school_year': '',
                    'match_type': 'no_match',
                    'has_address_data': False
                })
            
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def create_enriched_dataframe(self) -> pd.DataFrame:
        """Create the final enriched DataFrame"""
        self.logger.info("Creating enriched DataFrame...")
        
        if not self.enriched_data:
            self.logger.warning("No enriched data available")
            return pd.DataFrame()
        
        # Convert to DataFrame
        enriched_df = pd.DataFrame(self.enriched_data)
        
        # Reorder columns to put new address fields after existing ones
        firebase_columns = self.firebase_schools.columns.tolist()
        address_columns = [
            'address', 'city', 'state', 'county', 'zipcode', 
            'latitude', 'longitude', 'database_school_name', 
            'database_state_name', 'lea_name', 'school_status', 
            'data_year', 'school_year', 'match_type', 'has_address_data'
        ]
        
        # Ensure all columns exist
        for col in address_columns:
            if col not in enriched_df.columns:
                enriched_df[col] = ''
        
        # Reorder columns
        column_order = firebase_columns + address_columns
        enriched_df = enriched_df[column_order]
        
        self.logger.info(f"Created enriched DataFrame with {len(enriched_df)} records and {len(enriched_df.columns)} columns")
        
        return enriched_df
    
    def generate_summary_report(self):
        """Generate a summary report of the enrichment process"""
        print_section_header("FIREBASE SCHOOLS ENRICHMENT REPORT")
        
        stats = self.processing_stats
        
        print("📊 PROCESSING SUMMARY")
        print(f"Total CSV records loaded: {format_number(stats['total_records'])}")
        print(f"Unique schools processed: {format_number(stats['unique_schools'])}")
        print(f"Batches processed: {format_number(stats['processed_batches'])}")
        print(f"Database connection errors: {format_number(stats['database_errors'])}")
        
        print_section_header("Address Data Enrichment Results", "-")
        print(f"Schools with addresses: {format_number(stats['schools_with_addresses'])}")
        print(f"Schools without addresses: {format_number(stats['schools_without_addresses'])}")
        
        if stats['unique_schools'] > 0:
            coverage_pct = (stats['schools_with_addresses'] / stats['unique_schools']) * 100
            print(f"Address coverage: {coverage_pct:.1f}%")
            
            if coverage_pct >= 95:
                print("✅ EXCELLENT ADDRESS COVERAGE")
            elif coverage_pct >= 85:
                print("✅ GOOD ADDRESS COVERAGE")
            elif coverage_pct >= 70:
                print("⚠️  MODERATE ADDRESS COVERAGE")
            else:
                print("❌ LOW ADDRESS COVERAGE")
        
        print("\n🎯 DATA COLUMNS AVAILABLE IN OUTPUT:")
        print("Original columns:")
        if self.firebase_schools is not None:
            for col in self.firebase_schools.columns:
                print(f"  - {col}")
        
        print("\nEnriched address columns:")
        address_cols = [
            'address', 'city', 'state', 'county', 'zipcode', 
            'latitude', 'longitude', 'database_school_name', 
            'database_state_name', 'lea_name', 'school_status', 
            'data_year', 'school_year', 'match_type', 'has_address_data'
        ]
        for col in address_cols:
            print(f"  + {col}")
    
    def save_enriched_data(self, output_path: str):
        """Save the enriched data to CSV"""
        enriched_df = self.create_enriched_dataframe()
        
        if enriched_df.empty:
            self.logger.warning("No data to save")
            return
        
        # Save to CSV
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        enriched_df.to_csv(output_file, index=False)
        self.logger.info(f"✅ Enriched data saved to: {output_file}")
        
        return enriched_df
    
    def run_enrichment(self, output_path: str):
        """Run the complete enrichment process"""
        try:
            self.logger.info("🚀 Starting Firebase schools enrichment...")
            
            # Load Firebase data
            self.load_firebase_schools()
            
            # Connect to database
            self.connect_to_database()
            
            # Process schools in batches
            self.process_schools_in_batches()
            
            # Generate summary report
            self.generate_summary_report()
            
            # Save enriched data
            enriched_df = self.save_enriched_data(output_path)
            
            self.logger.info("✅ Firebase schools enrichment completed successfully!")
            
            return enriched_df
            
        except Exception as e:
            self.logger.error(f"❌ Enrichment failed: {str(e)}")
            raise
        finally:
            self._cleanup()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Enrich EDC schools data with address information")
    parser.add_argument('csv_file', nargs='?', 
                       default='../firebase_data/edc_schools.csv',
                       help='Path to EDC schools CSV file (default: ../firebase_data/edc_schools.csv)')
    parser.add_argument('-o', '--output', 
                       default=f'../reports/edc_schools_enriched_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                       help='Output CSV file path')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('-b', '--batch-size', type=int, default=100, 
                       help='Batch size for processing (default: 100)')
    
    args = parser.parse_args()
    
    # Verify CSV file exists
    if not Path(args.csv_file).exists():
        print(f"❌ CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    try:
        enricher = FirebaseSchoolsEnrichment(
            csv_file_path=args.csv_file,
            verbose=args.verbose,
            batch_size=args.batch_size
        )
        enriched_df = enricher.run_enrichment(args.output)
        
        print(f"\n🎉 SUCCESS! Enriched data saved to: {args.output}")
        print(f"📊 Final dataset: {len(enriched_df)} schools with {len(enriched_df.columns)} columns")
        
        return True
    except Exception as e:
        print(f"❌ Enrichment failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 