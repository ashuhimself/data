from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from module.task_definitions import create_tasks

dag = DAG(dag_id='dynamic_task_group_dag', 
          start_date=datetime(2024, 10, 7), 
          schedule_interval='@daily')

dynamic_task_group = TaskGroup(group_id='dynamic_task_group', dag=dag)

create_tasks(dag, dynamic_task_group)
