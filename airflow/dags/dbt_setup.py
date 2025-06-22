from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'juan_qversity',
    'start_date': datetime(2025, 6, 21),
    'depends_on_past': False,
}

# DAG definition for DBT setup pipeline: clean, deps, seed, debug
with DAG(
    dag_id='dbt_setup',
    default_args=default_args,
    schedule_interval=None,  # Manual execution only
    catchup=False,
    description='Initial DBT setup pipeline (clean, deps, seed, debug)',
    tags=['dbt', 'setup'],
) as dag:

    # Clean the dbt project (remove dbt_modules, target, etc.)
    dbt_clean = BashOperator(
        task_id='dbt_clean',
        bash_command='cd /opt/airflow/dbt && dbt clean',
    )

    # Install dbt dependencies
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps',
    )

    # Seed initial data into the database
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt && dbt seed',
    )

    # Debug DBT project and database connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt && dbt debug',
    )

    # Define task execution order
    dbt_clean >> dbt_deps >> dbt_seed >> dbt_debug
