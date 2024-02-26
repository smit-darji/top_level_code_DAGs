from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import logging


def fetching_ssm_parameter():
    session = boto3.session.Session()
    client = session.client('ssm', region_name='us-east-1')
    key_value = client.get_parameter(
        Name='/testing/toplevel/ssmparameter', WithDecryption=True
    ).get('Parameter').get('Value')
    logging.info(key_value)
    return key_value


ssm_parameter_call = fetching_ssm_parameter()


def print_fetching_ssm_parameter():
    ssm_parameter_call = fetching_ssm_parameter()
    logging.info(ssm_parameter_call)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}


dag = DAG(
    'example_top_level_code',
    default_args=default_args,
    description='DAG to test the top level code',
    schedule_interval=None
)

ssm_parameter_task = PythonOperator(
    task_id='top_level_code_task',
    python_callable=print_fetching_ssm_parameter,
    dag=dag,
)
