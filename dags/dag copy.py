from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
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

def task_function(task_name):
    print(f"Executing {task_name}")

@task_group(group_id='task_group_1', dag=dag)  # Ensure DAG is passed here
def my_task_group():
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_function,
        op_kwargs={'task_name': 'Task 1'},
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_function,
        op_kwargs={'task_name': 'Task 2'},
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=task_function,
        op_kwargs={'task_name': 'Task 3'},
    )

    task_4 = PythonOperator(
        task_id='task_4',
        python_callable=task_function,
        op_kwargs={'task_name': 'Task 4'},
    )

    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=task_function,
        op_kwargs={'task_name': 'Task 5'},
    )

    # Define dependencies within the task group
    task_1 >> [task_2, task_3]
    task_2 >> task_4
    task_3 >> task_4
    task_4 >> task_5

start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("Starting the DAG"),
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("Ending the DAG"),
    dag=dag
)

# Set up the overall DAG structure
start >> my_task_group() >> end
