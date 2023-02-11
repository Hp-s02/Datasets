from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def hello():
    print('Hello!, My First DAG Worked !!!')

args = {
    'owner':'Hema Prakash S',
    'start_date':datetime(2023, 2, 1)
}

dag = DAG(
    dag_id = 'dag1_func_defined_inside',
    default_args = args,
    schedule = '@daily'
)

with dag:
    hello_world = PythonOperator(
        task_id = 'hello',
        python_callable = hello
    )