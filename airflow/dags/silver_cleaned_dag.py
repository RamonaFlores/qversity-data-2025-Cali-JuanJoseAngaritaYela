from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'juan_qversity',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'sla': timedelta(minutes=30),
}

with DAG(
    dag_id='silver_cleaned_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # solo se ejecuta manualmente o vía trigger
    catchup=False,
    description='Limpieza avanzada y validación de calidad para la capa silver_cleaned',
    tags=['silver_cleaned', 'dbt', 'quality'],
) as dag:

    dbt_base_cmd = "cd /opt/airflow/dbt && dbt run --select"

    customers_cleaned = BashOperator(
        task_id='run_customers_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customers_cleaned",
    )

    billing_cleaned = BashOperator(
        task_id='run_customer_billing_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_billing_cleaned",
    )

    location_cleaned = BashOperator(
        task_id='run_customer_location_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_location_cleaned",
    )

    payments_cleaned = BashOperator(
        task_id='run_customer_payment_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_payment_cleaned",
    )

    services_cleaned = BashOperator(
        task_id='run_customer_services_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_services_cleaned",
    )

    devices_cleaned = BashOperator(
        task_id='run_devices_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.devices_cleaned",
    )

    # Dependencias entre tareas (ajustadas según uso de ref en DBT)
    customers_cleaned >> [
        billing_cleaned,
        location_cleaned,
        payments_cleaned,
        services_cleaned,
        devices_cleaned
    ]

