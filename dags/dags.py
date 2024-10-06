from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='example_task_group',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Create a task group using the @task_group decorator
@task_group
def my_task_group():
    @task
    def task_1():
        print("Task 1 executed")

    @task
    def task_2():
        print("Task 2 executed")

    # Define the order of task execution
    task_1() >> task_2()

# Create the task group in the DAG
my_task_group_instance = my_task_group()

# Define a downstream task if needed
@task
def final_task():
    print("Final task executed")

# Set task dependencies
my_task_group_instance >> final_task()
