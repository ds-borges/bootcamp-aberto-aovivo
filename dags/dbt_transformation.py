from airflow.decorators import dag, task
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

DBT_CLOUD_CONN_ID = "dbt-conn"
JOB_ID = "70471823545829"
@dag(
    dag_id="dbt_transformation",
    description="minha etl braba",
    schedule="@daily", #Cada 5 Min
    catchup=False #backfill
)
def running_dbt_cloud():
    rodar_dbt = DbtCloudRunJobOperator(
        task_id = "rodar_dbt",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=60,
        timeout=360,
    )

    rodar_dbt

running_dbt_cloud()