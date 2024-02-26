from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import ast

def has_top_level_code(file_path):
    with open(file_path, 'r') as f:
        code = f.read()
        try:
            parsed_ast = ast.parse(code)
            for node in parsed_ast.body:
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                    continue
                else:
                    return True
            return False
        except SyntaxError:
            return True

def analyze_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                is_top_level_code = has_top_level_code(file_path)
                print(f"File: {file_path}")
                if is_top_level_code:
                    print("Contains top-level code.")
                else:
                    print("Does not contain top-level code.")
                print()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'top_level_code_check',
    default_args=default_args,
    description='DAG to check for top-level code in Python files',
    schedule_interval=None
)

check_top_level_code_task = PythonOperator(
    task_id='check_top_level_code',
    python_callable=analyze_files,
    op_kwargs={'directory': '.'},  # Change this to the directory you want to analyze
    dag=dag,
)
