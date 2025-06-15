import os
from dotenv import load_dotenv
from arcgis.gis import GIS
from arcgis.geoenrichment import enrich
import json
from google.cloud import secretmanager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def access_secret_version(secret_ref):
    if secret_ref.startswith("sm://"):
        parts = secret_ref.split('/')
        project_id_or_number = parts[3]
        secret_id = parts[5]
        
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id_or_number}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    return secret_ref

def validate_percentages(percentages, tolerance=0.01):
    total = sum(percentages)
    return abs(1 - total) <= tolerance

def fetch_esri_data(latitude, longitude):
    load_dotenv()
    username = os.getenv('ESRI_USERNAME')
    password = os.getenv('ESRI_PASSWORD')
    url = os.getenv('ESRI_URL', 'https://www.arcgis.com')

    if not (username and password):
        logging.error("ESRI_USERNAME or ESRI_PASSWORD not found in .env file")
        return None

    try:
        # Process username and password through access_secret_version
        username = access_secret_version(username)
        password = access_secret_version(password)

        logging.info(f"Attempting to connect to {url} with username {username[:3]}*** and password {password[:3]}***")
        gis = GIS(url, username, password)
        logging.info(f"Successfully connected as: {gis.properties.user.username}")

        drive_times = [5, 10, 15]
        study_areas = [
            {
                "geometry": {"x": longitude, "y": latitude, "spatialReference": {"wkid": 4326}},
                "areaType": "NetworkServiceArea", 
                "bufferUnits": "Minutes", 
                "bufferRadii": [drive_time], 
            }
            for drive_time in drive_times
        ]

        enrichment_variables = [
            "AGE4_CY", #Current Year Total Population Age 4
            "AGE5_CY", #Current Year Total Population Age 5
            "AGE6_CY", #Current Year Total Population Age 6
            "AGE7_CY", #Current Year Total Population Age 7
            "AGE8_CY", #Current Year Total Population Age 8
            "AGE9_CY", #Current Year Total Population Age 9
            "AGE10_CY", #Current Year Total Population Age 10
            "AGE11_CY", #Current Year Total Population Age 11
            "AGE12_CY", #Current Year Total Population Age 12
            "AGE13_CY", #Current Year Total Population Age 13
            "AGE14_CY", #Current Year Total Population Age 14
            "AGE15_CY", #Current Year Total Population Age 15
            "AGE16_CY", #Current Year Total Population Age 16
            "AGE17_CY", #Current Year Total Population Age 17
            "AGE4_FY", #Future Year Total Population Age 4
            "AGE5_FY", #Future Year Total Population Age 5
            "AGE6_FY", #Future Year Total Population Age 6
            "AGE7_FY", #Future Year Total Population Age 7
            "AGE8_FY", #Future Year Total Population Age 8
            "AGE9_FY", #Future Year Total Population Age 9
            "AGE10_FY", #Future Year Total Population Age 10
            "AGE11_FY", #Future Year Total Population Age 11
            "AGE12_FY", #Future Year Total Population Age 12
            "AGE13_FY", #Future Year Total Population Age 13
            "AGE14_FY", #Future Year Total Population Age 14
            "AGE15_FY", #Future Year Total Population Age 15
            "AGE16_FY", #Future Year Total Population Age 16
            "AGE17_FY", #Future Year Total Population Age 17
            "AGE4C20", #2020 Total Population Age 4
            "AGE5C20", #2020 Total Population Age 5
            "AGE6C20", #2020 Total Population Age 6
            "AGE7C20", #2020 Total Population Age 7
            "AGE8C20", #2020 Total Population Age 8
            "AGE9C20", #2020 Total Population Age 9
            "AGE10C20", #2020 Total Population Age 10
            "AGE11C20", #2020 Total Population Age 11
            "AGE12C20", #2020 Total Population Age 12
            "AGE13C20", #2020 Total Population Age 13
            "AGE14C20", #2020 Total Population Age 14
            "AGE15C20", #2020 Total Population Age 15
            "AGE16C20", #2020 Total Population Age 16
            "AGE17C20", #2020 Total Population Age 17
            "NHADLTWH20", #2020 Non Hispanic White 18+ Population
            "NHADLTBL20", #2020 Non Hispanic Black 18+ Population
            "NHADLTAS20", #2020 Non Hispanic Asian 18+ Population
            "NHADLTPI20", #2020 Non Hispanic Pacific Islander 18+ Population
            "NHADLTAI20", #2020 Non Hispanic American Indian/Alaska Native 18+ Population
            "NHADLTOT20", #2020 Non Hispanic Other Race 18+ Population
            "NHADLT2R20", #2020 Non Hispanic Two or More Races 18+ Population
            "HADULTS20", #2020 Hispanic 18+ Population
            "NHWU18C20", #2020 Non Hispanic White Under 18 Population
            "NHBU18C20", #2020 Non Hispanic Black Under 18 Population
            "NHASU18C20", #2020 Non Hispanic Asian Under 18 Population
            "NHPIU18C20", #2020 Non Hispanic Pacific Islander Under 18 Population
            "NHAIU18C20", #2020 Non Hispanic American Indian/Alaska Native Under 18 Population
            "NHOU18C20", #2020 Non Hispanic Other Race Under 18 Population
            "NHMU18C20", #2020 Non Hispanic Two or More Races Under 18 Population
            "HU18RBS20", #2020 Hispanic Under 18 Population
            "MEDHINC_CY", #Current Year Median Household Income
            "HINCBASECY", #Current Year Households by Income Base
            "HINC0_CY", #Current Year Household Income less than $15,000
            "HINC15_CY", #Current Year Household Income $15,000-$24,999
            "HINC25_CY", #Current Year Household Income $25,000-$34,999
            "HINC35_CY", #Current Year Household Income $35,000-$49,999
            "TOTHU_CY", #Current Year Total Housing Units
            "RENTER_CY", #Current Year Renter Occupied Housing Units
            "VACANT_CY", #Current Year Vacant Housing Units
        ]

        logging.info("Sending enrich request...")
        result = enrich(study_areas=study_areas,
                        analysis_variables=enrichment_variables,
                        gis=gis)
        
        logging.info("Enrich request completed. Processing results...")
        if result.empty:
            logging.warning("No results returned from enrich request")
            return None
        
        logging.info("Column names in the ESRI response:")
        logging.info(result.columns.tolist())
        
        if not result.empty:
            esri_data = {}
            for index, row in result.iterrows():
                drive_time = drive_times[index]
                
                def safe_float(column_name):
                    column_name_lower = column_name.lower()
                    if column_name_lower in row.index:
                        try:
                            value = row[column_name_lower]
                            return float(value) if value is not None else 0.0
                        except (ValueError, TypeError):
                            logging.warning(f"Could not convert {column_name} to float. Value: {value}")
                            return 0.0
                    logging.warning(f"Column {column_name} not found in data.")
                    return 0.0

                def safe_percentage(part, whole):
                    return (part / whole) if whole != 0 else 0.0
                
                def safe_value(column_name, row):
                    if column_name.lower() in row.index:
                        value = row[column_name.lower()]
                        if isinstance(value, (int, float)):
                            return float(value)
                        return value
                    logging.warning(f"Column {column_name} not found in data.")
                    return None

                # Calculate racial group percentages for 2020 (18+ population)
                total_pop_adult_20 = sum(safe_float(f"nhadlt{race}20") for race in ['wh', 'bl', 'as', 'pi', 'ai', 'ot']) + safe_float("nhadlt2_r20") + safe_float("hadults20")
                
                adult_percentages = {
                    "per_hisp_adult_20": safe_percentage(safe_float("hadults20"), total_pop_adult_20),
                    "per_wht_adult_20": safe_percentage(safe_float("nhadltwh20"), total_pop_adult_20),
                    "per_blk_adult_20": safe_percentage(safe_float("nhadltbl20"), total_pop_adult_20),
                    "per_asn_adult_20": safe_percentage(safe_float("nhadltas20"), total_pop_adult_20),
                    "per_pi_adult_20": safe_percentage(safe_float("nhadltpi20"), total_pop_adult_20),
                    "per_ai_adult_20": safe_percentage(safe_float("nhadltai20"), total_pop_adult_20),
                    "per_other_adult_20": safe_percentage(safe_float("nhadltot20"), total_pop_adult_20),
                    "per_two_or_more_adult_20": safe_percentage(safe_float("nhadlt2_r20"), total_pop_adult_20),
                }

                # Validate adult percentages
                if not validate_percentages(adult_percentages.values()):
                    logging.warning(f"Adult percentages for drive time {drive_time} do not sum to 100% (sum: {sum(adult_percentages.values()):.2f})")

                # Calculate racial group percentages for 2020 (under 18 population)
                total_pop_child_20 = sum(safe_float(f"nh{race}u18_c20") for race in ['w', 'b', 'as', 'pi', 'ai', 'o', 'm']) + safe_float("hu18_rbs20")
                
                child_percentages = {
                    "per_hisp_child_20": safe_percentage(safe_float("hu18_rbs20"), total_pop_child_20),
                    "per_wht_child_20": safe_percentage(safe_float("nhwu18_c20"), total_pop_child_20),
                    "per_blk_child_20": safe_percentage(safe_float("nhbu18_c20"), total_pop_child_20),
                    "per_asn_child_20": safe_percentage(safe_float("nhasu18_c20"), total_pop_child_20),
                    "per_pi_child_20": safe_percentage(safe_float("nhpiu18_c20"), total_pop_child_20),
                    "per_ai_child_20": safe_percentage(safe_float("nhaiu18_c20"), total_pop_child_20),
                    "per_other_child_20": safe_percentage(safe_float("nhou18_c20"), total_pop_child_20),
                    "per_two_or_more_child_20": safe_percentage(safe_float("nhmu18_c20"), total_pop_child_20),
                }

                # Validate child percentages
                if not validate_percentages(child_percentages.values()):
                    logging.warning(f"Child percentages for drive time {drive_time} do not sum to 100% (sum: {sum(child_percentages.values()):.2f})")

                # Log total population counts
                logging.info(f"Drive Time {drive_time} minutes - Total Adult Population: {total_pop_adult_20:.0f}")
                logging.info(f"Drive Time {drive_time} minutes - Total Child Population: {total_pop_child_20:.0f}")

                # Calculate percentages for households $50K and below
                households_cy = safe_float("hincbasecy")
                households_50k_below_cy = sum(safe_float(f"hinc{i}_cy") for i in ['0', '15', '25', '35'])
                per_50k_cy = safe_percentage(households_50k_below_cy, households_cy)

                # Calculate percentages for renters and vacant units
                total_housing_units = safe_float("tothu_cy")
                renter_units = safe_float("renter_cy")
                vacant_units = safe_float("vacant_cy")
                per_renter_cy = safe_percentage(renter_units, total_housing_units)
                per_vacant_cy = safe_percentage(vacant_units, total_housing_units)

                # Log housing statistics
                logging.info(f"Drive Time {drive_time} minutes - Total Housing Units: {total_housing_units:.0f}")
                logging.info(f"Drive Time {drive_time} minutes - Renter Occupied Units: {renter_units:.0f}")
                logging.info(f"Drive Time {drive_time} minutes - Vacant Units: {vacant_units:.0f}")
                logging.info(f"Drive Time {drive_time} minutes - Renter Percentage: {per_renter_cy:.1%}")
                logging.info(f"Drive Time {drive_time} minutes - Vacancy Rate: {per_vacant_cy:.1%}")

                # Extract and process SHAPE data
                shape_column = next((col for col in row.index if col.lower() == 'shape'), None)
                if shape_column:
                    shape_data = row[shape_column]
                    logging.info(f"Shape data type: {type(shape_data)}")
                    logging.info(f"Shape data: {shape_data}")
                    
                    if isinstance(shape_data, dict):
                        drive_time_polygon = json.dumps(shape_data)
                    elif isinstance(shape_data, str):
                        try:
                            # If it's already a JSON string, parse and re-dump to ensure valid JSON
                            drive_time_polygon = json.dumps(json.loads(shape_data))
                        except json.JSONDecodeError:
                            logging.warning(f"Invalid JSON in SHAPE data: {shape_data[:100]}...")
                            drive_time_polygon = None
                    else:
                        logging.warning(f"Unexpected SHAPE data type: {type(shape_data)}")
                        drive_time_polygon = None
                else:
                    logging.warning("SHAPE column not found in ESRI response")
                    drive_time_polygon = None

                # Combine all data
                esri_data[drive_time] = {
                    **adult_percentages,
                    **child_percentages,
                    "per_50k_cy": per_50k_cy,
                    "per_renter_cy": per_renter_cy,
                    "per_vacant_cy": per_vacant_cy,
                    "drive_time_polygon": drive_time_polygon
                }

                # Add all the original ESRI variables to the esri_data dictionary
                for column in row.index:
                    if column.lower() != 'shape':
                        esri_data[drive_time][column] = safe_value(column, row)

            logging.info("ESRI data processing completed successfully")
            return esri_data

    except Exception as e:
        logging.error(f"Error fetching ESRI data: {str(e)}", exc_info=True)
        if 'result' in locals():
            logging.info("Result structure:")
            logging.info(result.info())
            if not result.empty:
                logging.info("First row of data:")
                logging.info(result.iloc[0])
        return None

if __name__ == "__main__":
    # Example usage
    test_latitude = 34.4939457
    test_longitude = -118.2160231
    result = fetch_esri_data(test_latitude, test_longitude)
    if result:
        logging.info("Processed ESRI data:")
        for drive_time, data in result.items():
            logging.info(f"\nDrive Time: {drive_time} minutes")
            for key, value in data.items():
                if key != "drive_time_polygon":
                    logging.info(f"{key}: {value}")
                else:
                    logging.info(f"{key}: [Polygon data available]")
    else:
        logging.error("Failed to fetch ESRI data.")