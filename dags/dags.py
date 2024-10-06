from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime

dag = DAG(
    dag_id='example_task_group',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

@dag.task_group
def my_task_group():
    @task
    def task_1():
        print("Task 1 executed")

    @task
    def task_2():
        print("Task 2 executed")

    task_1() >> task_2()

my_task_group_instance = my_task_group()

@task(dag=dag)
def final_task():
    print("Final task executed")

my_task_group_instance >> final_task()
