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

S3_URL=os.getenv("S3_URL")
LOCAL_PATH=os.getenv("LOCAL_PATH")
DB_CONN_STR=os.getenv("DB_CONN_STR")