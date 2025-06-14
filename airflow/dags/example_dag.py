from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

from airflow import DAG


def hello_world():
    print("Hello World from Airflow!")
    return "Hello World!"


default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "hello_world_dag",
    default_args=default_args,
    description="A simple hello world DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

hello_task = PythonOperator(
    task_id="hello_world_task", python_callable=hello_world, dag=dag
)
