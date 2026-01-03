from airflow.decorators import dag
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime

AIRBYTE_CONNETCION_ID = Variable.get("AIRBYTE_SAMPDATA_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'

@dag(
    dag_id="airbyte_dag_samp_data_postgres",
    description="minha etl braba2",
    schedule="@daily", #Cada 5 Min
    catchup=False #backfill
)
def running_airbyte():

    start_airbyte_sync = HttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte',
        endpoint=f'/v1/jobs',
        method='POST',
        headers={"Content-Type": "application/json",
             "User-Agent": "fake-useragent",
             "Accept": "application/json",
             "Authorization": API_KEY},
    data=json.dumps({"connectionId": AIRBYTE_CONNETCION_ID, "jobType": "sync"}),
    response_check=lambda response: response.json()['status'] == 'running'
    )

    start_airbyte_sync

running_airbyte()