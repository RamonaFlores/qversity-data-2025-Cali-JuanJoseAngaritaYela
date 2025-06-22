from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'juan_qversity',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'sla': timedelta(minutes=30),
}

# DAG definition for running advanced data cleaning and quality validation
# on the silver_cleaned layer using DBT
with DAG(
    dag_id='silver_cleaned_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # only triggered manually or via API
    catchup=False,
    description='Advanced cleaning and quality validation for the silver_cleaned layer',
    tags=['silver_cleaned', 'dbt', 'quality'],
) as dag:

    # Base DBT command prefix
    dbt_base_cmd = "cd /opt/airflow/dbt && dbt run --select"

    # Run model: customers_cleaned
    customers_cleaned = BashOperator(
        task_id='run_customers_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customers_cleaned",
    )

    # Run model: customer_billing_cleaned
    billing_cleaned = BashOperator(
        task_id='run_customer_billing_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_billing_cleaned",
    )

    # Run model: customer_location_cleaned
    location_cleaned = BashOperator(
        task_id='run_customer_location_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_location_cleaned",
    )

    # Run model: customer_payment_cleaned
    payments_cleaned = BashOperator(
        task_id='run_customer_payment_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_payments_cleaned",
    )

    # Run model: customer_services_cleaned
    services_cleaned = BashOperator(
        task_id='run_customer_services_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.customer_services_cleaned",
    )

    # Run model: devices_cleaned
    devices_cleaned = BashOperator(
        task_id='run_devices_cleaned',
        bash_command=f"{dbt_base_cmd} silver_cleaned.devices_cleaned",
    )

    # Define task dependencies based on DBT model references
    # All downstream models depend on the successful execution of customers_cleaned
    customers_cleaned >> [
        billing_cleaned,
        location_cleaned,
        payments_cleaned,
        services_cleaned,
        devices_cleaned
    ]
