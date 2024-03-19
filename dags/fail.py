import pendulum

from airflow import DAG
from airflow.decorators import task


def expensive_api_call():
    print("Hello from Airflow!")
    sleep(1000)


my_expensive_response = expensive_api_call()

with DAG(
    dag_id="example_python_operator",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    @task()
    def print_expensive_api_call():
        print(my_expensive_response)