from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup
import os
import subprocess

def run_python_script(script_path):
    """Executes a Python script located at the given path."""
    try:
        subprocess.run(['python', script_path], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to execute script {script_path}: {e}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),  # Adjust as needed
}

# Define the DAG
with DAG(
    'final_etl_dbt_dag_with_batch_logging',
    default_args=default_args,
    description='Combined DAG with batch logging: ETL (S3 to Redshift) and dbt Cloud job',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task: Start Batch Logging
    start_batch = PythonOperator(
        task_id='start_batch',
        python_callable=run_python_script,
        op_args=['/mnt/c/Nikhil/ETL_PYTHON/ETL_nikhil/01_python/parallel_scripts/start_batch.py'],
    )

    # Define TaskGroup: Oracle to S3
    with TaskGroup(group_id='oracle_to_s3') as oracle_to_s3:
        scripts_folder = "/mnt/c/Nikhil/ETL_PYTHON/ETL_nikhil/01_python/oracle_to_s3_scripts"
        scripts = [
            'customers.py',
            'employees.py',
            'offices.py',
            'orderdetails.py',
            'orders.py',
            'payments.py',
            'productlines.py',
            'products.py'
        ]

        tasks = []
        for script in scripts:
            task_id = f"run_{script.split('.')[0]}"
            script_path = os.path.join(scripts_folder, script)
            task = PythonOperator(
                task_id=task_id,
                python_callable=run_python_script,
                op_args=[script_path],
            )
            tasks.append(task)

    # Define TaskGroup: S3 to Redshift
    with TaskGroup(group_id='s3_to_redshift_stage') as s3_to_redshift:
        glue_jobs = [
            "offices_glue_job",
            "employees_glue_job",
            "customers_glue_job",
            "payments_glue_job",
            "orders_glue_job",
            "productlines_glue_job",
            "products_glue_job",
            "orderdetails_glue_job"
        ]

        tasks = []
        for job_name in glue_jobs:
            task = GlueJobOperator(
                task_id=f'trigger_{job_name}',
                job_name=job_name,
                region_name='eu-north-1',  # Replace with your AWS region
                aws_conn_id='aws_default'
            )
            tasks.append(task)

    # Task: Trigger dbt Cloud job
    run_dbt_job = DbtCloudRunJobOperator(
        task_id='redshift_stage_to_devdw',
        dbt_cloud_conn_id='dbt_cloud',  # Airflow connection ID for dbt Cloud
        job_id=70471823404564,  # Replace with your Job ID
        timeout=300,  # Optional: Set timeout in seconds
    )

    # Task: End Batch Logging
    end_batch = PythonOperator(
        task_id='end_batch',
        python_callable=run_python_script,
        op_args=['/mnt/c/Nikhil/ETL_PYTHON/ETL_nikhil/01_python/parallel_scripts/end_batch.py'],
    )

    # Define overall DAG dependencies
    start_batch >> oracle_to_s3 >> s3_to_redshift >> run_dbt_job >> end_batch

