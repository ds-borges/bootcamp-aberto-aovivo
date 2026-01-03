from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import json
import requests # Usamos requests direto para ter liberdade de gerar o token

# ID da Conexão (Fica fora pois é fixo)

# --- FUNÇÃO QUE GERA O TOKEN (O PAYLOAD VEM AQUI) ---
def get_new_token():

    client_id = Variable.get("AIRBYTE_CLIENT_ID")
    client_secret = Variable.get("AIRBYTE_CLIENT_SECRET")
    
    # 1. O Payload de Autenticação
    auth_payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    
    # 2. Solicita o Token novo
    response = requests.post(
        "https://api.airbyte.com/v1/applications/token",
        json=auth_payload
    )
    response.raise_for_status()
    return response.json()['access_token']

@dag(
    dag_id="airbyte_ingestion_samp_data_postgres",
    description="ETL Samp Data com Token Automatico",
    schedule="@daily",
    start_date=datetime(2024, 4, 18),
    catchup=False
)

def running_airbyte_samp_data():

    # Substituímos o HttpOperator por uma Task Python
    @task(task_id='start_airbyte_sync')
    def disparar_sync():
        
        AIRBYTE_CONNETCION_ID = Variable.get("AIRBYTE_SAMPDATA_POSTGRES_CONNECTION_ID")

        # 1. Gera o token fresco AGORA (na hora da execução)
        token = get_new_token()
        
        # 2. Monta o Header com o token novo
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}" # <--- Token inserido aqui dinamicamente
        }
        
        # 3. O Payload do Job (O que você já tinha)
        job_payload = {
            "connectionId": AIRBYTE_CONNETCION_ID,
            "jobType": "sync"
        }

        # 4. Faz o disparo
        print(f"🚀 Disparando sync para conexão {AIRBYTE_CONNETCION_ID}...")
        response = requests.post(
            "https://api.airbyte.com/v1/jobs",
            headers=headers,
            json=job_payload
        )
        
        # Validação (para não falhar se for 'pending')
        response.raise_for_status()
        status = response.json().get('status')
        
        # Aceita pending ou running
        if status in ['pending', 'running']:
            print(f"✅ Sucesso! Job iniciado com status: {status}")
        else:
            raise Exception(f"❌ Erro ao iniciar: Status {status}")
        
    # Chama a função
    disparar_sync()

running_airbyte_samp_data()