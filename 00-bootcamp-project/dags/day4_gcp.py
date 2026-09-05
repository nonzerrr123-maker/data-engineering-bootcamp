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


def get_clients_and_location(*, project_id, dataset_id, keyfile_path):
    """Return authenticated GCP clients and the real BigQuery dataset location."""
    credentials = load_credentials(keyfile_path)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    storage_client = storage.Client(project=project_id, credentials=credentials)

    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery_client.get_dataset(dataset_ref)
    print(f"BigQuery dataset {dataset_ref} location={dataset.location}")

    # Do not call storage.get_bucket() here. Some service accounts can read/write
    # objects but do not have storage.buckets.get, which used to make the final
    # task fail before BigQuery was even called.
    return bigquery_client, storage_client, dataset.location


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
    location,
    partition_field,
):
    """Fallback for GCS/BigQuery location incompatibility.

    Spark has already written the transformed parquet files to GCS. If BigQuery
    rejects a direct gs:// load because the bucket and dataset are in incompatible
    locations, download only the matching parquet part files and upload them to
    BigQuery through the API. This preserves the Day 4 flow and the exact partition
    being processed while avoiding the cross-location gs:// restriction.
    """
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
                    location=location,
                    rewind=True,
                )
                job.result()
        finally:
            try:
                os.remove(temp_path)
            except FileNotFoundError:
                pass


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

    bigquery_client, storage_client, location = get_clients_and_location(
        project_id=project_id,
        dataset_id=dataset_id,
        keyfile_path=keyfile_path,
    )

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    if partition_field and partition_date:
        table_id = f"{table_id}${partition_date.replace('-', '')}"

    config = _build_job_config(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        partition_field=partition_field,
    )

    print(f"Loading {source_uri} -> {table_id} in {location}")
    try:
        job = bigquery_client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=config,
            location=location,
        )
        job.result()
    except Exception as exc:
        # The most common remaining Day 4 failure here is GCS/BQ location
        # incompatibility. Keep the original exception visible, then retry via the
        # BigQuery upload API using the already-created parquet files.
        print(f"Direct load failed: {type(exc).__name__}: {exc}")
        _load_via_local_fallback(
            storage_client=storage_client,
            bigquery_client=bigquery_client,
            source_uri=source_uri,
            table_id=table_id,
            location=location,
            partition_field=partition_field,
        )

    base_table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery_client.get_table(base_table_id)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
        f"to {base_table_id}"
    )
