from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def test_bq():
    hook = BigQueryHook(gcp_conn_id="gcp_conn")
    client = hook.get_client()
    for d in client.list_datasets():
        print("DATASET:", d.dataset_id)

with DAG(
    dag_id="test_gcp_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="test_bigquery",
        python_callable=test_bq,
    )
