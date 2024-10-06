from airflow.operators.python import PythonOperator
from uti.task import read_yaml_file, generate_report, validate_data, transform_data, fetch_data_from_api, create_s3_upload_task

def create_tasks(dag, dynamic_task_group):
    read_yaml_task = PythonOperator(
        task_id='read_yaml_task',
        python_callable=read_yaml_file,
        dag=dag
    )

    generate_report_task = PythonOperator(
        task_id='generate_report_task',
        python_callable=generate_report,
        op_args=[[{'sales': 1000, 'region': 'East'}, {'sales': 1500, 'region': 'West'}]],
        dag=dag
    )

    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=validate_data,
        op_args=[[{'name': 'Ashutosh', 'age': 30}], ['name', 'age']],
        dag=dag
    )

    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
        op_args=[[{'name': 'Ashutosh', 'city': 'Bangalore'}, {'name': 'John', 'city': 'Jaunpur'}]],
        dag=dag
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_api,
        op_args=['https://jsonplaceholder.typicode.com/todos/1'],
        dag=dag
    )

    s3_upload_task = create_s3_upload_task(
        dag=dag,
        task_id='s3_upload_task',
        aws_conn_id='my_s3',
        s3_bucket='airflowashutosh',
        s3_key='test_files/sample.txt',
        data='This is a test file written directly to S3.',
        replace=True
    )

    dynamic_task_group.add(read_yaml_task)
    dynamic_task_group.add(generate_report_task)
    dynamic_task_group.add(validate_data_task)
    dynamic_task_group.add(transform_data_task)
    dynamic_task_group.add(fetch_data_task)
    dynamic_task_group.add(s3_upload_task)

