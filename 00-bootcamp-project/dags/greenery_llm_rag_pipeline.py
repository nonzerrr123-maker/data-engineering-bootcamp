import json
import os
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account

from greenery_rag_utils import build_context_chunks


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-b9bafacf-46f9-43ef-bcc")
SOURCE_DATASET_ID = os.getenv("GREENERY_SOURCE_DATASET_ID", "dbt_suntisuk_reporting")
SOURCE_TABLE_ID = os.getenv("GREENERY_SOURCE_TABLE_ID", "fct_orders")
TARGET_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "deb_bootcamp")
TARGET_TABLE_ID = os.getenv("GREENERY_RAG_TABLE_ID", "greenery_rag_context")
KEYFILE = os.getenv(
    "GCP_KEYFILE",
    "/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json",
)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2")
DAGS_FOLDER = Path("/opt/airflow/dags")
RAW_CONTEXT_FILE = DAGS_FOLDER / "greenery-summary-data.parquet"
EMBEDDED_CONTEXT_FILE = DAGS_FOLDER / "greenery-summary-data-with-embeddings.parquet"


def _bigquery_client():
    with open(KEYFILE, encoding="utf-8") as handle:
        info = json.load(handle)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)


def _require_gemini_key():
    if not GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Add a free-tier Google AI Studio key "
            "to 00-bootcamp-project/.env before running this DAG."
        )
    return GEMINI_API_KEY


def _candidate_source_datasets(preferred):
    candidates = [
        preferred,
        "dbt_suntisuk_reporting",
        "deb_bootcamp",
        "dbt_suntisuk",
    ]
    return list(dict.fromkeys(x for x in candidates if x))


def _read_fct_orders(client, dataset_id, table_id):
    errors = []
    for candidate in _candidate_source_datasets(dataset_id):
        relation = f"{GCP_PROJECT_ID}.{candidate}.{table_id}"
        query = f"""
            SELECT
                order_guid,
                order_created_at_utc,
                state,
                quantity,
                product_name,
                price
            FROM \`{relation}\`
            WHERE
                order_created_at_utc IS NOT NULL
                AND state IS NOT NULL
                AND product_name IS NOT NULL
        """
        try:
            rows = list(client.query(query).result())
        except Exception as exc:
            errors.append(f"{relation}: {type(exc).__name__}: {exc}")
            continue

        if not rows:
            errors.append(f"{relation}: table/query returned no rows")
            continue

        print(f"Using Greenery source table: {relation}")
        return pd.DataFrame([dict(row.items()) for row in rows])

    detail = "\n".join(errors)
    raise RuntimeError(
        "Could not read a usable fct_orders table from any expected dataset.\n" + detail
    )


def _gather_data(dataset_id=SOURCE_DATASET_ID, table_id=SOURCE_TABLE_ID, ds=None):
    if not ds:
        raise ValueError("Airflow ds is required")

    client = _bigquery_client()
    fct_orders = _read_fct_orders(client, dataset_id, table_id)
    contexts = build_context_chunks(fct_orders, processed_date=ds)
    contexts.to_parquet(RAW_CONTEXT_FILE, index=False)

    counts = contexts.groupby("chunk_type").size().to_dict()
    print(f"Created {len(contexts)} contextual chunks: {counts}")
    print(f"Wrote contextual data to {RAW_CONTEXT_FILE}")


def _get_embeddings():
    df = pd.read_parquet(RAW_CONTEXT_FILE)
    if df.empty:
        raise ValueError("Context parquet is empty")

    client = genai.Client(api_key=_require_gemini_key())
    embeddings = []

    batch_size = 20
    texts = df["text"].astype(str).tolist()
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        result = client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=batch,
        )
        if len(result.embeddings) != len(batch):
            raise RuntimeError(
                f"Embedding count mismatch: expected {len(batch)}, "
                f"got {len(result.embeddings)}"
            )
        embeddings.extend(item.values for item in result.embeddings)
        print(f"Embedded chunks {start + 1}-{start + len(batch)} of {len(texts)}")

    df["embedding"] = embeddings
    df.to_parquet(EMBEDDED_CONTEXT_FILE, index=False)
    print(f"Wrote embedded contextual data to {EMBEDDED_CONTEXT_FILE}")


def _load_data_to_bigquery():
    df = pd.read_parquet(EMBEDDED_CONTEXT_FILE)
    if df.empty:
        raise ValueError("Embedded context parquet is empty")

    client = _bigquery_client()
    destination = f"{GCP_PROJECT_ID}.{TARGET_DATASET_ID}.{TARGET_TABLE_ID}"

    schema = [
        bigquery.SchemaField("chunk_type", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("source_period", "STRING"),
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("processed_date", "DATE"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processed_date",
        ),
    )

    job = client.load_table_from_dataframe(df, destination, job_config=config)
    job.result()

    loaded = int(job.output_rows or 0)
    if loaded != len(df):
        raise RuntimeError(f"Expected to load {len(df)} rows, loaded {loaded}")

    print(f"Loaded {loaded} contextual chunks into {destination}")


with DAG(
    dag_id="greenery_llm_rag_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio", "greenery", "RAG", "week6-project"],
):
    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=_gather_data,
        op_kwargs={
            "dataset_id": SOURCE_DATASET_ID,
            "table_id": SOURCE_TABLE_ID,
            "ds": "{{ ds }}",
        },
    )

    get_embeddings = PythonOperator(
        task_id="get_embeddings",
        python_callable=_get_embeddings,
    )

    load_data_to_bigquery = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=_load_data_to_bigquery,
    )

    gather_data >> get_embeddings >> load_data_to_bigquery
