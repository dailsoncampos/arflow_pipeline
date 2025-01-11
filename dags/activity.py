import os
from airflow.models import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from dotenv import load_dotenv

load_dotenv()
data_path = os.getenv("DATA_PATH")

with DAG(
    'activity',
    start_date=pendulum.today('UTC').add(days=-2),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(task_id='task_1')
    task2 = EmptyOperator(task_id='task_2')
    task3 = EmptyOperator(task_id='task_3')

    task4 = BashOperator(
        task_id='create_folder',
        bash_command=f"mkdir -p /home/dailsoncampos_tech/airflow_pipeline/airflow_data={{data_interval_end}}"
    )

    task1 >> [task2, task3] 
    task3 >> task4
