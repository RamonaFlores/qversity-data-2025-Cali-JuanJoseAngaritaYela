"""
silver_transform DAG
~~~~~~~~~~~~~~~~~~~~
Orchestrates the steps of the Silver layer:
1. Extracts cleaned data from Bronze → optional staging.
2. Executes dbt run and dbt test for the silver folder.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks
default_args = {
    "owner": "juan_qversity",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition for running and testing all DBT models under the silver folder
with DAG(
    dag_id="silver_transform_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["silver", "customers"],
) as dag:

    # Run all models in the silver folder
    dbt_run = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver.*",
    )

    # Test all models in the silver folder
    dbt_test = BashOperator(
        task_id="dbt_test_silver",
        bash_command="cd /opt/airflow/dbt && dbt test --select silver.*",
    )

    # Ensure tests run only after the models are built
    dbt_run >> dbt_test
