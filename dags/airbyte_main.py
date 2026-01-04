#Ocultada no .airflowignore
from airflow.models import Variable
import requests

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

def running_airbyte(AIRBYTE_CONNETCION_ID):

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
            job_id = response.json().get('jobId')
            return job_id
        else:
            raise Exception(f"❌ Erro ao iniciar: Status {status}")