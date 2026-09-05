import json

from google.cloud import bigquery, storage
from google.oauth2 import service_account


def load_credentials(keyfile_path):
    with open(keyfile_path, encoding="utf-8") as f:
        service_account_info = json.load(f)
    return service_account.Credentials.from_service_account_info(service_account_info)


def get_bigquery_client_and_location(*, project_id, dataset_id, bucket_name, keyfile_path):
    """Return a BigQuery client and the dataset's real location.

    Day 4 used to hard-code asia-southeast1. That makes a load job fail immediately
    when the existing BigQuery dataset was created in another location. Resolve the
    destination dataset first and run the job in that exact location instead.
    """
    credentials = load_credentials(keyfile_path)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery_client.get_dataset(dataset_ref)
    dataset_location = dataset.location

    storage_client = storage.Client(project=project_id, credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    bucket_location = bucket.location

    print(
        f"BigQuery dataset {dataset_ref} location={dataset_location}; "
        f"GCS bucket gs://{bucket_name} location={bucket_location}"
    )

    return bigquery_client, dataset_location


def load_parquet_to_bigquery(
    *,
    project_id,
    dataset_id,
    table_name,
    bucket_name,
    source_uri,
    keyfile_path,
    partition_field=None,
    partition_date=None,
):
    bigquery_client, location = get_bigquery_client_and_location(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name=bucket_name,
        keyfile_path=keyfile_path,
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    if partition_field and partition_date:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        table_id = f"{table_id}${partition_date.replace('-', '')}"

    print(f"Loading {source_uri} -> {table_id} in {location}")
    job = bigquery_client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    base_table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery_client.get_table(base_table_id)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
        f"to {base_table_id}"
    )
