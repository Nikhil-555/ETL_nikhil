from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 27),  # Adjust as required
}

# Define the DAG
dag = DAG(
    'trigger_all_s3_to_stage_glue_jobs',  # DAG Name
    default_args=default_args,
    description='A DAG to trigger multiple AWS Glue jobs parallely',
    schedule_interval=None,  # Set to None to trigger manually
    catchup=False,
)

# List of Glue jobs
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

# Create tasks for each Glue job
tasks = []
for job_name in glue_jobs:
    task = GlueJobOperator(
        task_id=f'trigger_{job_name}',
        job_name=job_name,
        region_name='eu-north-1',  # Replace with your AWS region
        aws_conn_id='aws_default',  # Default AWS connection ID
        dag=dag,
    )
    tasks.append(task)

# Set dependencies for sequential execution
# for i in range(1, len(tasks)):
#     tasks[i - 1] >> tasks[i]

# If you want parallel execution, remove the dependencies:
# tasks will execute independently
