from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from jsonschema import validate, ValidationError
from dags.customer_schema import schema
from uuid import uuid4
import hashlib

# Load environment variables inside Docker container
load_dotenv("/opt/airflow/.env")

LOCAL_PATH = os.getenv("LOCAL_PATH")
DB_CONN_STR = os.getenv("DB_CONN_STR")

def hash_record(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

def download_json():
    try:
        S3_URL = os.getenv("S3_URL")
        logging.info(f"Downloading data from {S3_URL}")
        
        response = requests.get(S3_URL)
        response.raise_for_status()

        with open(LOCAL_PATH, "w") as f:
            f.write(response.text)

        logging.info(f"File saved at {LOCAL_PATH}")
    except Exception as e:
        logging.error(f"Failed to download file: {e}")
        raise

def load_to_postgres():

    try:
        logging.info(f"Reading file from {LOCAL_PATH}")
        with open(LOCAL_PATH) as f:
            data = json.load(f)

        #Initialize the valid and invalid json records lists
        #Invalid records will not be sent into postgres
        #they will be stored for revision

        valid_records=[]
        invalid_records=[]

        for record in data:
            try:
                #verifies if the record follow the schema structure
                validate(instance=record,schema=schema)

                # ✅ Improve data trazability by adding useful metadata
                valid_records.append({
                    "raw": json.dumps(record),
                    "record_validated": True,
                    "validation_error": None,
                    "uuid": str(uuid4()),
                    "record_hash": hash_record(record),
                    "ingestion_timestamp": datetime.utcnow(),
                    "source_system": "S3",
                    "source_file_name": os.path.basename(LOCAL_PATH),
                    "load_status": "loaded"
                })

            except ValidationError as e:
                #if it doesn't follow the structure, it's stored as an invalid record
                invalid_records.append({
                    "raw": json.dumps(record),
                    "record_validated": False,
                    "validation_error": str(e),
                    "uuid": str(uuid4()),
                    "record_hash": hash_record(record),
                    "ingestion_timestamp": datetime.utcnow(),
                    "source_system": "S3",
                    "source_file_name": os.path.basename(LOCAL_PATH),
                    "load_status": "failed"
                })

        #Useful logging, it allows to see the proportion of valid records
        logging.info(f"Valid records: {len(valid_records)}") 
        logging.info(f"Invalid records: {len(invalid_records)}")  

        # Combine valid and invalid records into a single DataFrame for full traceability

        all_records_df = pd.DataFrame(valid_records + invalid_records)

        engine = create_engine(DB_CONN_STR)

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        with engine.begin() as conn:
            all_records_df.to_sql(
                "customers_raw_json", con=conn, schema="bronze",
                if_exists="replace", index=False
            )

        logging.info("Data loaded into bronze.customers_raw_json")
    except Exception as e:
        logging.error(f"Failed to load data into Postgres: {e}")
        raise

# DAG config
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": lambda context: logging.error(f"Task {context['task_instance_key_str']} failed"),
}

with DAG(
    dag_id="ingest_customers_data_dag",
    default_args=default_args,
    description="Downloads the bronze layer JSON from S3 and loads it into PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id="download_json",
        python_callable=download_json
    )

    task_2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    task_1 >> task_2
