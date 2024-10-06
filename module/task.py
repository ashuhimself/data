import yaml 
import requests
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


def read_yaml_file():
    yaml_file_path = '/path/to/your/config.yaml'  # Replace with your actual YAML file path
    with open(yaml_file_path, 'r') as file:
        config_data = yaml.safe_load(file)

    # Access 'global' section
    global_section = config_data['global']
    file_name_format = global_section['file_name_format']
    s3_conn_id = global_section['s3_conn_d']
    
    return file_name_format, s3_conn_id


def generate_report(data):
    """
    Generates a summary report based on the input data.
    
    :param data: List of dictionaries representing rows of data
    :return: A string report summarizing the data
    """
    if data == None:
        return
    total_records = len(data)
    if total_records == 0:
        return "No records to generate a report."

    total_sales = sum(item['sales'] for item in data)
    regions = set(item['region'] for item in data)
    
    report = (
        f"Sales Report\n"
        f"Total Records: {total_records}\n"
        f"Total Sales: ${total_sales}\n"
        f"Regions Involved: {', '.join(regions)}\n"
    )
    return report

def validate_data(data, required_fields):
    if data == None or required_fields ==None:
        pass
    """
    Validates the input data by ensuring all required fields are present.
    
    :param data: List of dictionaries representing rows of data
    :param required_fields: List of required fields
    :return: True if valid, False otherwise
    """
    for record in data:
        for field in required_fields:
            if field not in record or record[field] is None:
                print(f"Validation failed for record: {record}")
                return False
    return True

def transform_data(data):
    if data == None:
        pass
    """
    Transforms the data by applying certain rules (e.g., converting strings to uppercase).
    
    :param data: List of dictionaries representing rows of data
    :return: Transformed data
    """
    transformed_data = []
    for record in data:
        new_record = {k: (v.upper() if isinstance(v, str) else v) for k, v in record.items()}
        new_record['transformed'] = True  # Add a new field
        transformed_data.append(new_record)
    
    return transformed_data


def fetch_data_from_api(api_url, params=None):
    if api_url == None or params == None:
        pass
    """
    Fetches data from a public API and returns the response data.
    
    :param api_url: The URL of the API to fetch data from
    :param params: Optional query parameters to send with the API request
    :return: Data fetched from the API
    """
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None
    


def create_s3_upload_task(dag, task_id, aws_conn_id, s3_bucket, s3_key, data, replace=True):
    """
    Creates an S3CreateObjectOperator task to upload data directly to an S3 bucket.
    
    :param dag: The Airflow DAG object to associate the task with
    :param task_id: The task ID for the S3 upload task
    :param aws_conn_id: The AWS connection ID to use for S3
    :param s3_bucket: The name of the S3 bucket
    :param s3_key: The object key (path) in the S3 bucket
    :param data: The content to upload to S3
    :param replace: Whether to replace the object if it exists (default is True)
    :return: An instance of the S3CreateObjectOperator task
    """
    return S3CreateObjectOperator(
        task_id=task_id,
        aws_conn_id=aws_conn_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data=data,
        replace=replace,
        dag=dag
    )


