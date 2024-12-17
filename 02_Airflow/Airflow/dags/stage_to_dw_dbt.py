from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.dates import days_ago

# Define default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Create DAG
with DAG(
    'trigger_dbt_cloud_job',
    default_args=default_args,
    description='Trigger a dbt Cloud job',
    schedule_interval=None,  # Set as per your requirement
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Trigger dbt Cloud Job
    run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_cloud_job',
        dbt_cloud_conn_id='dbt_cloud',  # Airflow connection ID for dbt Cloud
        job_id=70471823404564,  # Replace with your Job ID
        timeout=300,  # Optional: Set timeout in seconds
    )

    run_dbt_job
