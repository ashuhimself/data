# File: dags/dags.py

from airflow import DAG
from datetime import datetime
from uti.task_groups import create_tasks  # Import the task group function

# Define the DAG
dag = DAG(
    dag_id='dynamic_task_group_dag', 
    start_date=datetime(2024, 10, 7), 
    schedule_interval='@daily',
    catchup=False
)

# Create the task group by calling the imported function
task_group = create_tasks()

# This allows you to set the task group to the DAG
task_group.dag = dag
