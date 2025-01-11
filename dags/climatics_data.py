from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add

import pendulum
import os
from os.path import join
import pandas as pd
from dotenv import load_dotenv

with DAG(
    "climatics_data",
    start_date=pendulum.datetime(2022, 8, 22, tz="UTC"),
    schedule_interval='0 0 * * 1',
) as dag:
  task_one = BashOperator(
      task_id="task_one",
      bash_command=f"mkdir -p '/home/dailsoncampos_tech/airflow_pipeline/week={{data_interval_end.strftime('%Y-%m-%d')}}'",
  )

  def data_extraction(data_interval_end):
      load_dotenv()
      location = os.getenv("LOCATION")
      key_visual_crossing = os.getenv("KEY_VISUAL_CROSSING")

      city = location
      key = key_visual_crossing
      URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

      data = pd.read_csv(URL)

      file_path = f'/home/dailsoncampos_tech/airflow_pipeline/week={data_interval_end}/'

      data.to_csv(f'{file_path}original_data.csv')
      data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(f'{file_path}temperature.csv')
      data[['datetime', 'description', 'icon']].to_csv(f'{file_path}conditions.csv')

  task_two = PythonOperator(
    task_id = "extract_data",
    python_callable = data_extraction,
    op_args = {'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'}
  )

  task_one >> task_two