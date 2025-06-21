from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'juan_qversity',
    'start_date': datetime(2025, 6, 21),
    'depends_on_past': False,
}

with DAG(
    dag_id='dbt_setup',
    default_args=default_args,
    schedule_interval=None,  # Solo ejecución manual
    catchup=False,
    description='Pipeline inicial de setup DBT (clean, deps, seed, debug)',
    tags=['dbt', 'setup'],
) as dag:

    dbt_clean = BashOperator(
        task_id='dbt_clean',
        bash_command='cd /opt/airflow/dbt && dbt clean',
    )

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps',
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt && dbt seed',
    )

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt && dbt debug',
    )

    dbt_clean >> dbt_deps >> dbt_seed >> dbt_debug
