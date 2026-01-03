from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import time
import requests

AIRBYTE_TOKEN = Variable.get("AIRBYTE_API_TOKEN")

@dag(
    dag_id="orquestrador",
    schedule="@daily",
    start_date=datetime(2024, 4, 18),
    catchup=False
)
def orquestrador():

    @task
    def esperar_airbyte(connection_id_var):
        conn_id = Variable.get(connection_id_var)
        
        # --- CONFIGURAÇÃO DE SEGURANÇA ---
        limite_minutos = 60  # Vai tentar por 1 hora (60 x 1min)

        print(f"⏳ Monitorando conexão: {conn_id}. Limite: {limite_minutos} min.")

        for tentativa in range(limite_minutos):
            try:
                response = requests.post(
                    "https://api.airbyte.com/v1/jobs/list",
                    headers={"Authorization": f"Bearer {AIRBYTE_TOKEN}"},
                    json={"configId": conn_id, "pagination": {"pageSize": 1}}
                )
                response.raise_for_status() # Garante que erro de API (400/500) vai ser pego
                
                status = response.json()['data'][0]['status']
                job_id = response.json()['data'][0]['jobId']

                print(f"🔄 Minuto {tentativa + 1}/{limite_minutos} - Job {job_id}: {status.upper()}")

                if status == 'succeeded':
                    print("✅ Concluído com sucesso!"); 
                    return "Sucesso" # Sai da função e termina a tarefa verde
                
                elif status in ['failed', 'cancelled']:
                    raise Exception(f"❌ O Airbyte falhou com status: {status}")
                
            except Exception as e:
                print(f"⚠️ Erro temporário na verificação: {e}")

            # Se chegou aqui, ainda não acabou. Dorme 1 minuto.
            time.sleep(60)

        raise Exception(f"⏰ Timeout! O Airbyte demorou mais que {limite_minutos} minutos.")

    # --- DEFINIÇÃO DO FLUXO ---

    # 1. Samp Data
    start_samp = TriggerDagRunOperator(
        task_id="start_samp",
        trigger_dag_id="airbyte_ingestion_samp_data_postgres",
        wait_for_completion=True,
        poke_interval=30
    )
    wait_samp = esperar_airbyte("AIRBYTE_SAMPDATA_POSTGRES_CONNECTION_ID")

    # 2. Google Sheets
    start_google = TriggerDagRunOperator(
        task_id="start_google",
        trigger_dag_id="airbyte_ingestion_google_sheets_postgres",
        wait_for_completion=True,
        poke_interval=30
    )
    wait_google = esperar_airbyte("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")

    # 3. DBT
    start_dbt = TriggerDagRunOperator(
        task_id="start_dbt",
        trigger_dag_id="dbt_transformation",
        wait_for_completion=True
    )

    # Fluxo
    start_samp >> wait_samp >> start_google >> wait_google >> start_dbt

orquestrador()