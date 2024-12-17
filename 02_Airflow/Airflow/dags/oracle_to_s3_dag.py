from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'source_to_s3',
    default_args=default_args,
    description='ETL DAG to move data from Oracle to S3',
    schedule_interval=None,  # You can set to '@daily', 'cron', or custom interval
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the folder where Python scripts are located
scripts_folder = "/mnt/c/Nikhil/ETL_PYTHON/ETL_nikhil/01_python/oracle_to_s3_scripts"

# List of Python scripts to execute
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

# Create BashOperator tasks for each script
tasks = []
for script in scripts:
    task_id = f"run_{script.split('.')[0]}"
    script_path = os.path.join(scripts_folder, script)
    task = BashOperator(
        task_id=task_id,
        bash_command=f"python {script_path}",
        dag=dag
    )
    tasks.append(task)

# Define dependencies (all scripts run independently)
# for i in range(len(tasks) - 1):
#     tasks[i] >> tasks[i + 1]  # Change this to parallelize: `tasks[0].set_downstream(tasks[1:])`

#for task in tasks:
#    dag >> task
