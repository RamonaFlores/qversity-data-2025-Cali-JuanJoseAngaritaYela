from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'juan_qversity',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=15),
}

with DAG(
    dag_id='silver_cleaned_tests_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # ejecución manual
    catchup=False,
    description='Valida la calidad de datos en la capa silver_cleaned con DBT tests',
    tags=['silver_cleaned', 'dbt', 'data_quality'],
) as dag:

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests_silver_cleaned',
        bash_command='cd /opt/airflow/dbt && dbt test --select silver_cleaned.*',
    )
