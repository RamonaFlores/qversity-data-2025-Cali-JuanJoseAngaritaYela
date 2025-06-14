from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

#Let's load our .env variables ! inside the docker container

load_dotenv("/opt/airflow/.env")
# URL where the JSON is located at Amazon Simple Storage Service (or ...S3 haha)

S3_URL=os.getenv("S3_URL")
#the downloaded data file path, where it will be stored inside the container
LOCAL_PATH=os.getenv("LOCAL_PATH")
#postgresql connection string
DB_CONN_STR=os.getenv("DB_CONN_STR")



def download_json():
    #performs  a GET REQUEST at S3
    response=requests.get(S3_URL)
    #Throws an error if the request fails
    response.raise_for_status()

    #saves the content of the JSON file
    with open(LOCAL_PATH, "w") as f:
        f.write(response.text)
    
    print(f"File saved at:{LOCAL_PATH}")

