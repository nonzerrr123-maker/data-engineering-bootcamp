import csv
import json

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone
from google.cloud import bigquery, storage
from google.oauth2 import service_account

BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "project-b9bafacf-46f9-43ef-bcc"
BUCKET_NAME = "deb-bootcamp-005-non"
DAGS_FOLDER = "/opt/airflow/dags"
KEYFILE_PATH = "/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json"
DATA = "addresses"

def _extract_data():
    url = f"http://34.87.139.82:8000/{DATA}/"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    data = response.json()
    with open(f"{DAGS_FOLDER}/{DATA}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        header = [
            "address_id",
            "address",
            "zipcode",
            "state",
            "country",
        ]
        writer.writerow(header)
        for each in data:
            row = [
                each["address_id"],
                each["address"],
                each["zipcode"],
                each["state"],
                each["country"],
            ]
            writer.writerow(row)

def _load_data_to_gcs():
    service_account_info = json.load(open(KEYFILE_PATH))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(project=GCP_PROJECT_ID, credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)
    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    bucket.blob(destination_blob_name).upload_from_filename(file_path)

def _load_data_from_gcs_to_bigquery():
    service_account_info = json.load(open(KEYFILE_PATH))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials, location=LOCATION)
    source_uri = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{DATA}/*.parquet"
    table_id = f"{GCP_PROJECT_ID}.deb_bootcamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job = bigquery_client.load_table_from_uri(source_uri, table_id, job_config=job_config, location=LOCATION)
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

default_args = {"owner": "airflow", "start_date": timezone.datetime(2021, 2, 9)}

with DAG(
    dag_id="greenery_addresses_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):
    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data)
    load_data_to_gcs = PythonOperator(task_id="load_data_to_gcs", python_callable=_load_data_to_gcs)
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/spark/pyspark/greenery_transform.py",
        conn_id="my_spark",
        application_args=["--table", DATA, "--bucket", BUCKET_NAME, "--keyfile", KEYFILE_PATH],
    )
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery
