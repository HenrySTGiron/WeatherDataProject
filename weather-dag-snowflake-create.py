import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import snowflake.connector
import os

os.environ['no_proxy']='*'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def create_climate_schema_and_table():
    try:
        conn = snowflake.connector.connect(
            user='USER',
            password='PASSWORD',
            account='ACCOUNT'
            )
        logger.info('Connection successful')
    except snowflake.connector.errors.DatabaseError as e:
        logger.error('Error connecting to Snowflake: %s', e)

    cursor = conn.cursor()
    create_warehouse_sql = """
    CREATE WAREHOUSE IF NOT EXISTS Climate_Fashion_warehouse
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 1 
    AUTO_RESUME = TRUE 
    """
    cursor.execute(create_warehouse_sql)
    # Create Database
    create_database_sql = 'CREATE DATABASE IF NOT EXISTS Climate_Fashion_DB'
    cursor.execute(create_database_sql)

    # Use Database
    use_database_sql = 'USE DATABASE Climate_Fashion_DB'
    cursor.execute(use_database_sql)

    # Create Schema
    create_schema_sql = 'CREATE SCHEMA IF NOT EXISTS Climate_Schema'
    cursor.execute(create_schema_sql)

    logger.info('Tables have begun to be created!')

    create_date_table_sql = '''
    CREATE OR REPLACE TABLE Climate_Schema.Date_Table (
    date_id INT PRIMARY KEY,
    date_value DATE,
    season VARCHAR(20)
    );
    '''
    cursor.execute(create_date_table_sql)
    logger.info('Date table created!')

    create_station_table_sql = '''
    CREATE OR REPLACE TABLE Climate_Schema.Station_Table (
    station_id VARCHAR(100) PRIMARY KEY,
    zipcode VARCHAR(10),
    city VARCHAR(50),
    state VARCHAR(2)
    );
    '''
    cursor.execute(create_station_table_sql)
    logger.info('Station table created!')

    create_fact_table_sql = '''
    CREATE OR REPLACE TABLE Climate_Schema.Climate_Fact_Table (
    date_id INT ,
    station_id VARCHAR(100),
    min_temp INT,
    max_temp INT,
    snowfall INT,
    snow_depth INT,
    rain INT,
    is_training_data BOOLEAN,
    FOREIGN KEY (date_id) REFERENCES Climate_Schema.Date_Table(date_id),
    FOREIGN KEY (station_id) REFERENCES Climate_Schema.Station_Table(station_id)
    );
    '''
    cursor.execute(create_fact_table_sql)
    logger.info('Fact table created!')

    sql_stage = '''
    CREATE OR REPLACE STAGE json_stage
        DIRECTORY = (ENABLE = TRUE);'''

    cursor.execute(sql_stage)
    logger.info('Stage created!')
    conn.close()
    logger.info('Connection closed!')

def create_fashion_schema_and_table():
    try:
        conn = snowflake.connector.connect(
            user='USER',
            password='PASSWORD',
            account='ACCOUNT',
            warehouse='Climate_Fashion_warehouse',
            database='Climate_Fashion_DB'
            )
        logger.info('Connection successful')
    except snowflake.connector.errors.DatabaseError as e:
        logger.error('Error connecting to Snowflake: %s', e)

    cursor = conn.cursor()

    use_database_sql = 'USE DATABASE Climate_Fashion_DB'
    cursor.execute(use_database_sql)

    create_schema_sql = 'CREATE OR REPLACE SCHEMA Fashion_Schema'
    cursor.execute(create_schema_sql)

    logger.info('Tables are being created!')

    create_customer_stats_table_sql = '''
    CREATE OR REPLACE TABLE Fashion_Schema.Customer_Stats_Table (
    customer_id INT PRIMARY KEY,
    statistic_id INT,
    statistic_value VARCHAR(255)
    );
    '''
    cursor.execute(create_customer_stats_table_sql)
    logger.info('Customer stats table created!')

    create_customer_address_table_sql = """
    CREATE OR REPLACE TABLE Fashion_Schema.Customer_Address_Table (
    customer_id INT,
    address_id INT PRIMARY KEY,
    primary_address BOOLEAN,
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zipcode VARCHAR(20),
    country VARCHAR(100)
    );
    """
    cursor.execute(create_customer_address_table_sql)
    logger.info('Customer address table created!')

    create_customer_table_sql = """
    CREATE OR REPLACE TABLE Fashion_Schema.Customer_Table (
    customer_id INT PRIMARY KEY,
    address_id INT,
    FOREIGN KEY (customer_id) REFERENCES Fashion_Schema.Customer_Stats_Table(customer_id),
    FOREIGN KEY (address_id) REFERENCES Fashion_Schema.Customer_Address_Table(address_id)
    );
    """
    cursor.execute(create_customer_table_sql)
    logger.info('Customer table created!')

    create_order_line_table_sql = """
    CREATE OR REPLACE TABLE Fashion_Schema.Order_Line_Table (
    order_id INT PRIMARY KEY,
    order_line_id INT,
    item_id INT,
    order_date TIMESTAMP_NTZ
    );
    """
    cursor.execute(create_order_line_table_sql)
    logger.info('Order line table created!')

    create_order_table_sql = """
    CREATE OR REPLACE TABLE Fashion_Schema.Order_Table (
    order_date TIMESTAMP_NTZ PRIMARY KEY,
    order_id INT,
    customer_id INT,
    FOREIGN KEY (customer_id) REFERENCES Fashion_Schema.Customer_Table(customer_id),
    FOREIGN KEY (order_id) REFERENCES Fashion_Schema.Order_Line_Table(order_id)
    );
    """
    cursor.execute(create_order_table_sql)
    logger.info('Order table created!')

   

with DAG('snowflake-stage-create-dag', 
    default_args=default_args,
    schedule_interval = '@daily',
    catchup = False) as dag:


    create_climate_infra = PythonOperator(
        task_id = 'snowflake_climate_infra_task',
        python_callable = create_climate_schema_and_table,
        dag = dag
    ) 
    create_fashion_infra = PythonOperator(
        task_id = 'snowflake_fashion_infra_task',
        python_callable = create_fashion_schema_and_table,
        dag = dag
    )
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_extract_dag',
        trigger_dag_id='weather_api_extract_and_transform_dag',
        dag=dag,
        trigger_rule='all_success'
    )

    [create_climate_infra, create_fashion_infra] >> trigger_dag