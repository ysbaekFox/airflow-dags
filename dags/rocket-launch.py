import json
import pathlib

import airflow
import requests

import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BsshOperator
from airflow.operators.python import PythonOperator

dag=DAG(
    dag_id="donwload_rocket_laaunches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches=BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.josn -L 'http://ll.thespacedevs.com/2.0.0/launch/upcoming'", dag=dag,
)

def _get_pictures():
    pass

get_pictures=PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify=BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images"',
    dag=dag,
)

download_launches >> get_pictures >> notify