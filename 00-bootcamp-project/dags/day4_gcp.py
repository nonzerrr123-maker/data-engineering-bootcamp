import json
import os
import tempfile
from urllib.parse import urlparse

from google.cloud import bigquery, storage
from google.oauth2 import service_account


def load_credentials(keyfile_path):
    with open(keyfile_path, encoding="utf-8") as f:
        service_account_info = json.load(f)
    return service_account.Credentials.from_service_account_info(service_account_info)


def get_clients(*, project_id, keyfile_path):
    """Return authenticated clients without reading dataset metadata.

    The Day 4 service account may be allowed to run load jobs and modify tables
    without having bigquery.datasets.get. BigQuery can infer a job location from
    the destination dataset/table when location is omitted, so reading dataset
    metadata here is unnecessary and can itself cause a 403.
    """
    credentials = load_credentials(keyfile_path)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    storage_client = storage.Client(project=project_id, credentials=credentials)
    return bigquery_client, storage_client


def _build_job_config(*, write_disposition, partition_field=None):
    config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    if partition_field:
        config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    return config


def _load_via_local_fallback(
    *,
    storage_client,
    bigquery_client,
    source_uri,
    table_id,
    partition_field,
):
    """Upload matching parquet files through the BigQuery API as a fallback."""
    parsed = urlparse(source_uri)
    if parsed.scheme != "gs":
        raise ValueError(f"Unsupported source URI: {source_uri}")

    bucket_name = parsed.netloc
    object_pattern = parsed.path.lstrip("/")
    prefix = object_pattern.split("*", 1)[0]
    bucket = storage_client.bucket(bucket_name)
    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket, prefix=prefix)
        if blob.name.endswith(".parquet")
    ]

    if not blobs:
        raise FileNotFoundError(f"No parquet files found for {source_uri}")

    print(
        f"Direct GCS->BigQuery load failed; falling back to API upload for "
        f"{len(blobs)} parquet file(s)."
    )

    total_output_rows = 0
    for index, blob in enumerate(blobs):
        fd, temp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        try:
            blob.download_to_filename(temp_path)
            disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if index == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )
            config = _build_job_config(
                write_disposition=disposition,
                partition_field=partition_field,
            )
            with open(temp_path, "rb") as parquet_file:
                job = bigquery_client.load_table_from_file(
                    parquet_file,
                    table_id,
                    job_config=config,
                    rewind=True,
                )
                job.result()
                total_output_rows += job.output_rows or 0
        finally:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass

    return total_output_rows


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
    del bucket_name  # bucket name is already encoded in source_uri

    bigquery_client, storage_client = get_clients(
        project_id=project_id,
        keyfile_path=keyfile_path,
    )

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    if partition_field and partition_date:
        table_id = f"{table_id}${partition_date.replace('-', '')}"

    config = _build_job_config(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        partition_field=partition_field,
    )

    # Intentionally omit location. BigQuery infers it from the destination
    # dataset/table. This avoids requiring bigquery.datasets.get just to discover
    # the dataset location.
    print(f"Loading {source_uri} -> {table_id}")
    try:
        job = bigquery_client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=config,
        )
        job.result()
        output_rows = job.output_rows or 0
        print(f"Loaded {output_rows} row(s) to {table_id}")
    except Exception as exc:
        print(f"Direct load failed: {type(exc).__name__}: {exc}")
        output_rows = _load_via_local_fallback(
            storage_client=storage_client,
            bigquery_client=bigquery_client,
            source_uri=source_uri,
            table_id=table_id,
            partition_field=partition_field,
        )
        print(f"Loaded {output_rows} row(s) to {table_id} via API upload fallback")
