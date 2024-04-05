import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import snowflake.connector
import requests
import json
import os

# Needed to make sure airflow run on ARM M1 chip
os.environ["no_proxy"] = "*"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Boston_ZipCodes = {'02130': 'GHCND:USC00193890', '02128': 'GHCND:USW00014739'}
City = 'Boston'
State = 'MA'
header_token = {'token': 'iGBXTgpcgwHMTlSJNXfcYtnkYdESTQrf'}
start_date = '2023-03-01'
end_date = '2024-03-01'

conn = snowflake.connector.connect(
    user='USER',
    password='PASSWORD',
    account='ACCOUNT',
    warehouse='Climate_Fashion_warehouse',
    database='Climate_Fashion_DB',
    schema='Public'
)
cursor = conn.cursor()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 4),
    'email': 'henryhungryhen@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Function to determine season based on month
def get_season(month):
    if 3 <= month <= 5:
        return 'Spring'
    elif 6 <= month <= 8:
        return 'Summer'
    elif 9 <= month <= 11:
        return 'Fall'
    else:
        return 'Winter'

def generate_date_dimension(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    data = []
    date_id = 1

    current_date = start_date
    while current_date <= end_date:
        data.append({
            'date_id': date_id,
            'date_value': current_date.strftime('%Y-%m-%d'),
            'season': get_season(current_date.month)
        })
        current_date += timedelta(days=1)
        date_id += 1

    return data

def create_date_dimension_data_file():
    # Generate JSON data
    json_data = generate_date_dimension(start_date, end_date)

    # Generate filename based on start date, end date, and "date_dimension"
    file_name = f'{start_date}_{end_date}_date_dimension.json'
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, file_name)

    # Write JSON data to a file
    with open(file_path, 'w') as file:
        json.dump(json_data, file, indent=4)

    logger.info(f'JSON data file "{file_name}" created successfully.')

def generate_station_dimension(zipcode_station_dict, city, state):
    station_data = []
    
    for zipcode, station_id in zipcode_station_dict.items():
        station_info = {
            "station_id": station_id,
            "zipcode": zipcode,
            "city": city,
            "state": state
        }
        station_data.append(station_info)
    return station_data

def create_station_data_dimension_file():
    station_data = generate_station_dimension(Boston_ZipCodes, City, State)
    current_directory = os.getcwd()

    zipcode = next(iter(Boston_ZipCodes.keys()))
    station_id = next(iter(Boston_ZipCodes.values()))

    file_name = f'{zipcode}_{station_id}_station_dimension_data.json'
    file_path = os.path.join(current_directory, file_name)

    with open(file_path, 'w') as file:
        json.dump(station_data, file, indent=4)
    logger.info(f'Station data JSON file created successfully: {file_name}')

def transform_json(json_data):
    transformed_data = {}
    for item in json_data:
        date = item["date"]
        station = item["station"]
        datatype = item["datatype"]
        value = item["value"]
        
        # Create a unique key for each date and station combination
        key = (date, station)
        
        # Check if the key already exists in the transformed data
        if key not in transformed_data:
            # Create a new dictionary for the date and station combination
            transformed_data[key] = {
                "date": date,
                "station": station,
                "TMIN": None,
                "TMAX": None,
                "SNOW": None,
                "SNWD": None,
                "PRCP": None,
                "is_training_data": 'true'
            }
        
        # Update the values based on the datatype
        if datatype == "PRCP":
            transformed_data[key]["PRCP"] = value
        elif datatype == "TMIN":
            transformed_data[key]["TMIN"] = value
        elif datatype == "TMAX":
            transformed_data[key]["TMAX"] = value
        elif datatype == "SNOW":
            transformed_data[key]["SNOW"] = value
        elif datatype == "SNWD":
            transformed_data[key]["SNWD"] = value
    
    return list(transformed_data.values())

import logging

def weather_api_extract_and_transform():
    for zip_code, station_id in Boston_ZipCodes.items():
        URL = f'https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid={station_id}&limit=50&unit=metric&startdate={start_date}&enddate={end_date}'
        r = requests.get(URL, headers=header_token)

        if r.status_code == 200:
            try:
                parsed_data = r.json()
                if 'results' in parsed_data:
                    weather_dicts = parsed_data['results']
                    logger.info('Got JSON data')
                    logger.debug(weather_dicts)
                    current_directory = os.getcwd()

                    file_name = f'{zip_code}__{station_id}_{start_date}_{end_date}.json'
                    file_path = os.path.join(current_directory, file_name)
                
                    with open(file_path, 'w') as file:
                        cleaned_data = transform_json(weather_dicts)
                        json.dump(cleaned_data, file)
                    logger.info("JSON data saved to file: %s", file_path)
                    try:
                        put_sql = f"PUT 'file://{file_path}' @public.json_stage"
                        cursor.execute(put_sql)
                        logger.info('JSON data in stage, code: %s', cursor.sfqid)
                    except Exception as e:
                        logger.error("Error executing query: %s", e)
                else:
                    logger.info('No weather data found')
            except ValueError:
                logger.error('Failed to parse JSON data')
        else:
            logger.error('Error: Failed to retrieve data (status code: %s)', r.status_code)

with DAG('weather_api_extract_and_transform_dag', 
    default_args=default_args,
    schedule_interval = '@daily',
    catchup = False) as dag:


    extract_and_transform_data = PythonOperator(
    task_id = 'weather_api_extract_and_transform_task',
    python_callable = weather_api_extract_and_transform,
    dag = dag
    )

    create_date_data = PythonOperator(
    task_id = 'create_date_data_dimension_task',
    python_callable = create_date_dimension_data_file,
    dag = dag
    )

    create_station_data = PythonOperator(
    task_id = 'create_station_data_dimension_task',
    python_callable = create_station_data_dimension_file,
    dag = dag
    )

    trigger_dag = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='weather_load_dag',
    dag=dag,
    trigger_rule='all_success'
    )

    [extract_and_transform_data, create_date_data, create_station_data] >> trigger_dag