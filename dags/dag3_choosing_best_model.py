from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import randint
from datetime import datetime

def training_model():
    return randint(70,100)

def choose_best_model(ti):
    accuracies = ti.xcom_pull(
        task_ids = [
            'Training_Model_A',
            'Training_Model_B',
            'Training_Model_C',
        ]
    )
    best_accuracy = max(accuracies)

    if best_accuracy>90:
        return 'Accurate'
    return 'Inaccurate'

def accuratef():
    print('Accurate')

def inaccuratef():
    print('Inaccurate')

args = {
    'owner':'Hema Prakash S',
    'start_date':datetime(2023, 2, 1)
}

dag = DAG(
    dag_id = 'dag3_choosing_best_model',
    default_args=args,
    schedule = '@daily'
)

with dag:
    training_model_A = PythonOperator(
        task_id = 'Training_Model_A',
        python_callable=training_model
    )

    training_model_B = PythonOperator(
        task_id = 'Training_Model_B',
        python_callable=training_model
    )

    training_model_C = PythonOperator(
        task_id = 'Training_Model_C',
        python_callable=training_model
    )

    choosing_model = BranchPythonOperator(
        task_id = 'Choose_Best_Model',
        python_callable = choose_best_model
    )

    accurate = PythonOperator(
        task_id = 'Accurate',
        python_callable = accuratef
    )

    inaccurate = PythonOperator(
        task_id = 'Inaccurate',
        python_callable = inaccuratef
    )

    [training_model_A, training_model_B, training_model_C] >> choosing_model >> [accurate, inaccurate]