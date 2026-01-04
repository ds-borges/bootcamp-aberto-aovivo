from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import time

try:
    from airbyte_main import get_new_token
except ImportError:
    from dags.airbyte_main import get_new_token

@dag(
    dag_id="orquestrador",
    schedule="@daily",
    start_date=datetime(2024, 4, 18),
    catchup=False
)
def orquestrador():

    @task(task_id="esperar_intervalo")
    def dormir(minutos: int):
        print(f"😴 Iniciando soneca de {minutos} minutos...")
        time.sleep(minutos * 60)
        print("⏰ Acordei! Seguindo o fluxo.") #Verificar se o timer ta sendo seguido

    # 1. Samp Data
    start_samp = TriggerDagRunOperator(
        task_id="start_samp",
        trigger_dag_id="airbyte_ingestion_samp_data_postgres",
        wait_for_completion=True,
        poke_interval=30
    )

    # 2. Google Sheets
    start_google = TriggerDagRunOperator(
        task_id="start_google",
        trigger_dag_id="airbyte_ingestion_google_sheets_postgres",
        wait_for_completion=True,
        poke_interval=30
    )
    # 3. DBT
    start_dbt = TriggerDagRunOperator(
        task_id="start_dbt",
        trigger_dag_id="dbt_transformation",
        wait_for_completion=True
    )

    # Fluxo
    start_google >> dormir(8) >> start_samp >> dormir(8) >> start_dbt

orquestrador()