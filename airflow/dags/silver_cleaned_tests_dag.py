from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'juan_qversity',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=15),
}

# DAG definition for executing DBT tests on the silver_cleaned layer
with DAG(
    dag_id='silver_cleaned_tests_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # manual execution only
    catchup=False,
    description='Validates data quality in the silver_cleaned layer using DBT tests',
    tags=['silver_cleaned', 'dbt', 'data_quality'],
) as dag:

    # Run DBT tests on all models in the silver_cleaned layer
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests_silver_cleaned',
        bash_command='cd /opt/airflow/dbt && dbt test --select silver_cleaned.*',
    )
