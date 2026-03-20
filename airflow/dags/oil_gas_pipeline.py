from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

with DAG("oil_gas_pipeline", start_date=datetime(2024,1,1), schedule_interval="@daily", catchup=False) as dag:

    bronze = DatabricksSubmitRunOperator(
        task_id="bronze",
        json={"notebook_task": {"notebook_path": "/Repos/bronze"}}
    )

    silver = DatabricksSubmitRunOperator(
        task_id="silver",
        json={"notebook_task": {"notebook_path": "/Repos/silver"}}
    )

    gold = DatabricksSubmitRunOperator(
        task_id="gold",
        json={"notebook_task": {"notebook_path": "/Repos/gold"}}
    )

    bronze >> silver >> gold