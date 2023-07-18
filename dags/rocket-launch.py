import json
import pathlib

import airflow
import requests

import requests.exceptions as requests_exceptions

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

ROCKET_LAUNCHES_URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming/"
JSON_PATH = '/tmp/launches.json'
TARGET_DIR = '/tmp/images'

dag=DAG(
    dag_id="donwload_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
        task_id='download_launches',
        bash_command="curl -o ./launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming/'",
        dag=dag,
)

def _get_pictures():
    # Path() : Path 객체 생성
    # mkdir() - exist_ok=True : 폴더가 없을 경우 자동으로 생성
    pathlib.Path("./images").mkdir(parents=True, exist_ok=True)

    # launches.json 파일에 있는 모든 그림 파일 download
    with open("./launches.json") as f:
        launches = json.load(f)
        image_urls = [launch['image'] for launch in launches['results']]

        for i, image_url in enumerate(image_urls):
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f"./images/{image_filename}"

                with open(target_file, 'wb') as f:
                    f.write(response.content)
                print(f'Downloaded {image_url} to {target_file}')
            except requests_exceptions.MissingSchema:
                print(f'{image_url} appears to be an invalid URL.')
            except requests_exceptions.ConnectionError:
                print(f'Could not connect to {image_url}.')


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "Hello World"',
    dag=dag,
)


download_launches >> get_pictures >> notify
