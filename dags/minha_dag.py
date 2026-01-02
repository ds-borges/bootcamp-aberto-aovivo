from time import sleep
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="minha_dag",
    description="minha etl braba",
    schedule="*/5 * * * *", #Cada 5 Min
    start_date=datetime(2026,1,2),
    catchup=False #backfill
)
def pipeline():

    @task
    def primeira_atividade():
        print("primeira atividade rodou com sucesso")
        sleep(2)

    @task
    def segunda_atividade():
        print("segunda atividade rodou com sucesso")
        sleep(2)

    @task
    def terceira_atividade():
        print("terceira atividade rodou com sucesso")
        sleep(2)

    t1 = primeira_atividade()
    t2 = segunda_atividade()
    t3 = terceira_atividade()

    t1 >> t2 >> t3

pipeline()