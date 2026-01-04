from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from airbyte_main import running_airbyte

@dag(
    dag_id="airbyte_ingestion_samp_data_postgres",
    description="ETL Samp Data com Token Automatico",
    schedule="@daily",
    start_date=datetime(2024, 4, 18),
    catchup=False
)

def running_airbyte_samp_data():

    @task(task_id='start_airbyte_sync')
    def disparar_sync():
        
        conn_id = Variable.get("AIRBYTE_SAMPDATA_POSTGRES_CONNECTION_ID")
        job_id = running_airbyte(conn_id)
        
        print(f"Tarefa finalizada para o Job ID: {job_id}")

    disparar_sync()

running_airbyte_samp_data()