from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import snowflake.connector
import json
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.environ["no_proxy"]="*"

conn = snowflake.connector.connect(
    user='user',
    password='password',
    account='account',
    warehouse='CLIMATE_FASHION_WAREHOUSE',
    database='Climate_Fashion_DB',
    schema='Climate_Schema'
)
cursor = conn.cursor()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 4),
    'email':'henryhungryhen@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def final_cleanup():
    cwd = os.getcwd()
    logger.info("Current working directory: %s", cwd)
    file_path = os.path.join(os.getcwd(), '02128_GHCND_USW00014739_2023-03-01_2024-03-10.json')

    with open('2023-03-01_2024-03-10_date_dimension.json', 'r') as date_file:
        date_dimension_data = json.load(date_file)
        logger.info('Opened date dimension file!')
    # Create a dictionary mapping date_value to date_id
    date_id_map = {entry['date_value']: entry['date_id'] for entry in date_dimension_data}

    # Original JSON data
    with open(file_path, 'r') as climate_fact_file:
        climate_fact_data = json.load(climate_fact_file)
        logger.info('Opened climate fact file!')

    # Replace "date" field with "date_id" based on the mapping and remove the "date" key
    for entry in climate_fact_data:
        date_value = entry.pop('date').split('T')[0]
        entry['date_id'] = date_id_map.get(date_value, None)

    # Ensure 'is_training_data' remains boolean
    for entry in climate_fact_data:
        entry['is_training_data'] = bool(entry['is_training_data'])
        
    # Rearrange such that it can be inserted into the fact table    
arranged_data = []
    for entry in climate_fact_data:
        arranged_entry = {
        'date_id': entry['date_id'],
        'station_id': entry['station'],
        "min_temp": entry["TMIN"],
        "max_temp": entry["TMAX"],
        "snowfall": entry["SNOW"],
        "snow_depth": entry.get("SNWD", 0),  # Handle missing SNWD with default 0
        "rain": entry.get("PRCP", 0),  # Handle missing PRCP with default 0
        "is_training_data": entry["is_training_data"] 
        }
        arranged_data.append(arranged_entry)

    # Write the modified JSON data to a new file
    with open('modified_json_data.json', 'w') as output_file:
        json.dump(arranged_data, output_file, indent=4)

    logger.info('Modified JSON data file created successfully.')
def weather_load():
    try:
        create_json_file_format = '''
        CREATE OR REPLACE FILE FORMAT my_json_format
        TYPE = 'JSON'
        COMPRESSION = 'AUTO'
        STRIP_OUTER_ARRAY = TRUE;
        '''
        cursor.execute(create_json_file_format)

        check_file_existence = 'LIST @PUBLIC.JSON_STAGE'
        cursor.execute(check_file_existence)

        results = cursor.fetchall()
        for row in results:
            logger.info(row[0])

        copy_into_sql = '''
        COPY INTO CLIMATE_FACT_TABLE
        FROM @PUBLIC.JSON_STAGE
        FILES = ('modified_json_data.json')
        FILE_FORMAT = my_json_format
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR=ABORT_STATEMENT
        '''

        cursor.execute(copy_into_sql)
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error("Error loading data: %s", e)
    finally:
        cursor.close()
        conn.close()

with DAG('weather_load_dag', default_args=default_args, schedule_interval=None) as dag:
    
    final_climate_data_cleanup =PythonOperator(
        task_id='final_climate_data_cleanup_task',
        python_callable=final_cleanup,
        dag=dag
    )
    
    transform_data = PythonOperator(
        task_id='weather-load-task',
        python_callable=weather_load,
        dag=dag
    )

final_climate_data_cleanup >> transform_data
