from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG("sample_dag", schedule_interval="@daily") as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> end
