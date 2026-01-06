from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from pendulum import datetime
import uuid
from airflow.operators.bash import BashOperator
from airflow.models import Variable

@dag(
    dag_id="gcs_to_bq_dataproc_ecommerce_v2",
    description="ETL Ecommerce: GCS a BigQuery y Transformación con Dataproc",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "us-east1", "pyspark"],
)
def gcs_to_bq_dataproc_ecommerce():

    # Extraemos el ID del proyecto de tu variable de Airflow
    project_id = Variable.get("gcpproject_id_new")
    dataset    = "retail_data"      
    bucket     = "ecommerce-bucket01"
    temp_bucket = "bq-temp-gds-ecommerce"    

    # 1) Cargar productos -> BigQuery
    load_products = GCSToBigQueryOperator(
        task_id="load_products",
        gcp_conn_id="gcp_conn",
        bucket=bucket,
        source_objects=["datasets/products/products.json"],
        destination_project_dataset_table=f"{project_id}:{dataset}.products",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        # IMPORTANTE: Aquí usamos "US" porque es la localidad de tu dataset
        location="US", 
    )

    # 2) Cargar órdenes -> BigQuery
    load_orders = GCSToBigQueryOperator(
        task_id="load_orders",
        gcp_conn_id="gcp_conn",
        bucket=bucket,
        source_objects=["datasets/orders/orders.json"],
        destination_project_dataset_table=f"{project_id}:{dataset}.orders",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        location="US",
    )

    batch_id = f"ecom-transform-{str(uuid.uuid4())[:8]}"

    # 3) Ejecutar Spark en Dataproc Serverless
    run_dataproc = DataprocCreateBatchOperator(
        task_id="run_dataproc_transform_join",
        gcp_conn_id="gcp_conn",
        project_id=project_id,
        # Dataproc NECESITA una región física. Tus buckets están en us-east1.
        region="us-east1", 
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{bucket}/scripts/spark_retail_transformation.py",
                "args": [
                    "--project", project_id,
                    "--dataset", dataset,
                    "--temp_bucket", f"gs://{temp_bucket}"
                ],
                "jar_file_uris": []
            },
            "runtime_config": {
                "version": "2.2",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "airflow-local@n8n-proyectos-483201.iam.gserviceaccount.com",
                    "network_uri": f"projects/{project_id}/global/networks/default",
                    "subnetwork_uri": f"projects/{project_id}/regions/us-east1/subnetworks/default",
                }
            },
        },
        batch_id=batch_id,
    )

    dummy_message = BashOperator(
            task_id='Dummy_Message_OP',
            bash_command=f'echo "Job Done en proyecto {project_id} !!!"',
        )

    # Orden de las tareas
    [load_products, load_orders] >> run_dataproc >> dummy_message

dag = gcs_to_bq_dataproc_ecommerce()