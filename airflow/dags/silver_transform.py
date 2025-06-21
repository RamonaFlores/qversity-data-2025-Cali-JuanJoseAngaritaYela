"""
silver_transform DAG
~~~~~~~~~~~~~~~~~~~~
Orquesta los pasos de Silver:
1. Extrae datos limpios de Bronze → staging temporal (opcional).
2. Ejecuta dbt run y dbt test para la carpeta silver.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "juan_qversity",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_transform_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["silver", "customers"],
) as dag:

    # Ejecuta solo los modelos silver
    dbt_run = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver.*",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_silver",
        bash_command="cd /opt/airflow/dbt && dbt test --select silver.*",
    )

    dbt_run >> dbt_test
