from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup
import os

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
    'combined_etl_dbt_dag',
    default_args=default_args,
    description='Combined DAG: ETL (S3 to Redshift) and dbt Cloud job',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Overall start and end DummyOperators
    start_dag = DummyOperator(task_id='start_dag')
    end_dag = DummyOperator(task_id='end_dag')

    # Define TaskGroup: Oracle to S3
    with TaskGroup(group_id='oracle_to_s3') as oracle_to_s3:
        start_oracle_to_s3 = DummyOperator(task_id='start_oracle_to_s3')
        end_oracle_to_s3 = DummyOperator(task_id='end_oracle_to_s3')

        # Define Bash tasks for Oracle to S3 scripts
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
            task = BashOperator(
                task_id=task_id,
                bash_command=f"python {script_path}"
            )
            tasks.append(task)

        # Set TaskGroup dependencies
        start_oracle_to_s3 >> tasks >> end_oracle_to_s3

    # Define TaskGroup: S3 to Redshift
    with TaskGroup(group_id='s3_to_redshift') as s3_to_redshift:
        start_s3_to_redshift = DummyOperator(task_id='start_s3_to_redshift')
        end_s3_to_redshift = DummyOperator(task_id='end_s3_to_redshift')

        # Define Glue tasks for S3 to Redshift jobs
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

        # Set TaskGroup dependencies
        start_s3_to_redshift >> tasks >> end_s3_to_redshift

    # Task: Trigger dbt Cloud job
    run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_cloud_job',
        dbt_cloud_conn_id='dbt_cloud',  # Airflow connection ID for dbt Cloud
        job_id=70471823404564,  # Replace with your Job ID
        timeout=300,  # Optional: Set timeout in seconds
    )

    # Define overall DAG dependencies
    start_dag >> oracle_to_s3 >> s3_to_redshift >> run_dbt_job >> end_dag

