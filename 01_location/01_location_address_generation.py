#!/usr/bin/env python3
"""
School Location Data Generation Script

This script reads school names from a CSV file and uses the Google Maps API
to retrieve location information including address, city, state, county, 
latitude, and longitude.
"""

import os
import csv
import pandas as pd
import googlemaps
from dotenv import load_dotenv
import time
from pathlib import Path

# Load environment variables
load_dotenv()

def get_location_data(school_name, gmaps_client):
    """
    Get location data for a school using Google Maps API
    
    Args:
        school_name (str): Name of the school
        gmaps_client: Google Maps client instance
        
    Returns:
        dict: Location data including address, city, state, county, lat, lng
    """
    try:
        # Search for the school
        geocode_result = gmaps_client.geocode(school_name)
        
        if not geocode_result:
            return {
                'school_name': school_name,
                'address': 'Not found',
                'city': 'Not found',
                'state': 'Not found',
                'county': 'Not found',
                'latitude': None,
                'longitude': None,
                'status': 'Not found'
            }
        
        # Get the first result
        result = geocode_result[0]
        
        # Extract coordinates
        location = result['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        
        # Extract address components
        address_components = result['address_components']
        formatted_address = result['formatted_address']
        
        # Initialize variables
        street_number = ''
        route = ''
        city = ''
        state = ''
        county = ''
        postal_code = ''
        
        # Parse address components
        for component in address_components:
            types = component['types']
            
            if 'street_number' in types:
                street_number = component['long_name']
            elif 'route' in types:
                route = component['long_name']
            elif 'locality' in types:
                city = component['long_name']
            elif 'sublocality_level_1' in types and not city:
                # Handle NYC boroughs and other sublocalities
                city = component['long_name']
            elif 'administrative_area_level_1' in types:
                state = component['short_name']
            elif 'administrative_area_level_2' in types:
                county = component['long_name']
            elif 'postal_code' in types:
                postal_code = component['long_name']
        
        # Special handling for NYC - if we have Kings County but no city, it's Brooklyn
        if not city and county and state == 'NY':
            if 'Kings' in county:
                city = 'Brooklyn'
            elif 'New York' in county:
                city = 'Manhattan'
            elif 'Queens' in county:
                city = 'Queens'
            elif 'Bronx' in county:
                city = 'Bronx'
            elif 'Richmond' in county:
                city = 'Staten Island'
        
        # Construct street address
        street_address = f"{street_number} {route}".strip()
        if not street_address:
            street_address = formatted_address.split(',')[0]
        
        return {
            'school_name': school_name,
            'address': street_address,
            'city': city,
            'state': state,
            'county': county,
            'latitude': lat,
            'longitude': lng,
            'status': 'Success'
        }
        
    except Exception as e:
        print(f"Error processing {school_name}: {str(e)}")
        return {
            'school_name': school_name,
            'address': 'Error',
            'city': 'Error',
            'state': 'Error',
            'county': 'Error',
            'latitude': None,
            'longitude': None,
            'status': f'Error: {str(e)}'
        }

def main():
    """Main function to process school locations"""
    
    # Check for Google Maps API key
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        print("Error: GOOGLE_MAPS_API_KEY not found in environment variables")
        print("Please create a .env file in the project root with your Google Maps API key")
        return
    
    # Initialize Google Maps client
    gmaps = googlemaps.Client(key=api_key)
    
    # Define paths
    input_file = 'edc_schools/new_schools/remaining_CSGF.csv'
    output_dir = Path('01_location/output')
    output_file = output_dir / 'school_locations.csv'
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        return
    
    # Read school names from CSV
    school_names = []
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:  # Skip empty rows
                school_names.append(row[0].strip())
    
    print(f"Found {len(school_names)} schools to process")
    
    # Process each school
    results = []
    for i, school_name in enumerate(school_names, 1):
        print(f"Processing {i}/{len(school_names)}: {school_name}")
        
        location_data = get_location_data(school_name, gmaps)
        results.append(location_data)
        
        # Add a small delay to avoid hitting API rate limits
        time.sleep(0.1)
        
        # Print status for every 10 schools
        if i % 10 == 0:
            success_count = sum(1 for r in results if r['status'] == 'Success')
            print(f"Processed {i} schools. Success rate: {success_count}/{i} ({success_count/i*100:.1f}%)")
    
    # Save results to CSV
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    
    # Print summary
    success_count = sum(1 for r in results if r['status'] == 'Success')
    not_found_count = sum(1 for r in results if r['status'] == 'Not found')
    error_count = len(results) - success_count - not_found_count
    
    print(f"\n=== Processing Complete ===")
    print(f"Total schools processed: {len(results)}")
    print(f"Successfully geocoded: {success_count}")
    print(f"Not found: {not_found_count}")
    print(f"Errors: {error_count}")
    print(f"Success rate: {success_count/len(results)*100:.1f}%")
    print(f"Results saved to: {output_file}")
    
    # Show sample of results
    print(f"\nSample results:")
    print(df.head().to_string(index=False))

if __name__ == "__main__":
    main() 