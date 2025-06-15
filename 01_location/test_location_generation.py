#!/usr/bin/env python3
"""
Test version of School Location Data Generation Script
This processes only the first 3 schools for testing
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
    """Get location data for a school using Google Maps API"""
    try:
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
        
        result = geocode_result[0]
        location = result['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        
        address_components = result['address_components']
        formatted_address = result['formatted_address']
        
        street_number = ''
        route = ''
        city = ''
        state = ''
        county = ''
        
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
    """Test with just a few schools"""
    
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        print("Error: GOOGLE_MAPS_API_KEY not found in environment variables")
        print("Please create a .env file in the project root with GOOGLE_MAPS_API_KEY=your_api_key_here")
        return
    
    gmaps = googlemaps.Client(key=api_key)
    
    # Test with just 3 schools
    test_schools = [
        "Achievement First Ujima High School",
        "Frederick Douglass Campus", 
        "Idaho Novus Classical Academy"
    ]
    
    output_dir = Path('01_location/output')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'test_school_locations.csv'
    
    print(f"Testing with {len(test_schools)} schools")
    
    results = []
    for i, school_name in enumerate(test_schools, 1):
        print(f"Processing {i}/{len(test_schools)}: {school_name}")
        location_data = get_location_data(school_name, gmaps)
        results.append(location_data)
        time.sleep(0.2)  # Small delay
    
    # Save results
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    
    # Print results
    print(f"\n=== Test Results ===")
    print(df.to_string(index=False))
    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    main() 