
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple function that will be executed
def print_hello():
    print("Hello, World!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 5),  # Specify the start date for the DAG
    'retries': 1,  # Number of retries for failed tasks
}

# Create the DAG object
with DAG(
    dag_id='hello_world_dag',  # Unique identifier for the DAG
    default_args=default_args,
    schedule_interval='@daily',  # Schedule the DAG to run daily
    catchup=False,  # Do not run past scheduled runs
) as dag:

    # Define a task using PythonOperator
    hello_task = PythonOperator(
        task_id='print_hello',  # Unique identifier for the task
        python_callable=print_hello,  # Function to be called
    )

    # Define the task dependencies
    hello_task  # In this case, there's only one task
