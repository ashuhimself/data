
from airflow import DAG
from datetime import datetime
from uti.task_groups import create_tasks 

dag = DAG(
    dag_id='dynamic_task_group_dag', 
    start_date=datetime(2024, 10, 7), 
    schedule_interval='@daily',
    catchup=False
)

task_group = create_tasks()

task_group.dag = dag
