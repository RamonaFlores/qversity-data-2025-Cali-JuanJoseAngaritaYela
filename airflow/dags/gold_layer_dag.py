from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_run_gold_layer",
    default_args=default_args,
    description="Run and test the gold layer models in DBT",
    schedule_interval=None,  # o usa "@daily" si quieres automatizar
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "gold", "analytics"],
) as dag:

    dbt_run_gold = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command="""
        cd /dbt/qversity-data-2025-Cali-JuanJoseAngaritaYela &&
        dbt run --select gold
        """,
    )

    dbt_test_gold = BashOperator(
        task_id="test_dbt_gold_models",
        bash_command="""
        cd /dbt/qversity-data-2025-Cali-JuanJoseAngaritaYela &&
        dbt test --select gold
        """,
    )

    dbt_run_gold >> dbt_test_gold
