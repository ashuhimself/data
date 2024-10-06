from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'task_group_example',
    default_args=default_args,
    description='A DAG with a task group containing 5 tasks',
    schedule_interval=timedelta(days=1),
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

@task_group(group_id='task_group_1', dag=dag)
def tg1():
    @task()
    def task_1():
        print("Executing Task 1")

    @task()
    def task_2():
        print("Executing Task 2")

    @task()
    def task_3():
        print("Executing Task 3")

    @task()
    def task_4():
        print("Executing Task 4")

    @task()
    def task_5():
        print("Executing Task 5")

    # Define the task dependencies within the group
    task_1_result = task_1()
    task_2_result = task_2()
    task_3_result = task_3()
    task_4_result = task_4()
    task_5_result = task_5()

    task_1_result >> [task_2_result, task_3_result] >> task_4_result >> task_5_result

tg1_group = tg1()

# Set up the overall DAG structure
start >> tg1_group >> end