import csv
import json

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone
from google.cloud import storage
from google.oauth2 import service_account

from day4_gcp import load_parquet_to_bigquery

BUSINESS_DOMAIN = "greenery"
GCP_PROJECT_ID = "project-b9bafacf-46f9-43ef-bcc"
DATASET_ID = "deb_bootcamp"
BUCKET_NAME = "deb-bootcamp-005-non"
DAGS_FOLDER = "/opt/airflow/dags"
KEYFILE_PATH = "/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json"
DATA = "users"


def _extract_data(ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    data = response.json()
    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w", newline="") as f:
            writer = csv.writer(f)
            header = [
                "user_id", "first_name", "last_name", "email", "phone_number",
                "created_at", "updated_at", "address_id",
            ]
            writer.writerow(header)
            for each in data:
                address_id = each.get("address_id", each.get("address"))
                writer.writerow([
                    each.get("user_id"), each.get("first_name"), each.get("last_name"), each.get("email"),
                    each.get("phone_number"), each.get("created_at"), each.get("updated_at"), address_id,
                ])
        return "load_data_to_gcs"
    return "do_nothing"


def _load_data_to_gcs(ds):
    service_account_info = json.load(open(KEYFILE_PATH))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(project=GCP_PROJECT_ID, credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)
    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}-{ds}.csv"
    bucket.blob(destination_blob_name).upload_from_filename(file_path)


def _load_data_from_gcs_to_bigquery(ds):
    source_uri = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/ds={ds}/*.parquet"
    load_parquet_to_bigquery(
        project_id=GCP_PROJECT_ID,
        dataset_id=DATASET_ID,
        table_name=DATA,
        bucket_name=BUCKET_NAME,
        source_uri=source_uri,
        keyfile_path=KEYFILE_PATH,
        partition_field="created_at",
        partition_date=ds,
    )


default_args = {"owner": "airflow", "start_date": timezone.datetime(2020, 1, 5)}

with DAG(
    dag_id="greenery_users_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):
    start = EmptyOperator(task_id="start")
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={"ds": "{{ ds }}"},
    )
    do_nothing = EmptyOperator(task_id="do_nothing")
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/spark/pyspark/greenery_transform.py",
        conn_id="my_spark",
        application_args=["--table", DATA, "--bucket", BUCKET_NAME, "--keyfile", KEYFILE_PATH, "--ds", "{{ ds }}"],
    )
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )
    end = EmptyOperator(task_id="end", trigger_rule="one_success")
    start >> extract_data
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end
