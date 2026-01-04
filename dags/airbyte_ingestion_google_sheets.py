from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from airbyte_main import running_airbyte

@dag(
    dag_id="airbyte_ingestion_google_sheets_postgres",
    description="minha etl braba",
    schedule="@daily", #Cada 5 Min
    start_date=datetime(2024, 4, 18),
    catchup=False #backfill
)
def running_airbyte_google_sheets():

    @task(task_id='start_airbyte_sync')
    def disparar_sync():
        
        conn_id = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
        job_id = running_airbyte(conn_id)
        
        print(f"Tarefa finalizada para o Job ID: {job_id}")    
    
    disparar_sync()

running_airbyte_google_sheets()