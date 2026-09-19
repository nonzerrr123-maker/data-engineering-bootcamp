import json
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-b9bafacf-46f9-43ef-bcc")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "deb_bootcamp")
TABLE_ID = os.getenv("WEEK6_COURSE_TABLE_ID", "course_embeddings")
KEYFILE = os.getenv(
    "GCP_KEYFILE",
    "/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json",
)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")
DAGS_FOLDER = "/opt/airflow/dags"

COURSE_DATA = [
    "คอร์ส Probability for Data Science - ความน่าจะเป็นถูกนำมาใช้ในงาน Data Science ในงานด้านวิเคราะห์ข้อมูลและสร้างโมเดล เพื่อทำให้มั่นใจได้ว่าข้อมูลที่ได้มามีความหมายและมี Insight รวมถึงการนำข้อมูลไปสร้างโมเดล Machine Learning และการทำนาย",
    "คอร์ส Data Pipelines with Airflow - คอร์สสำหรับ Data Engineer ที่สอนการสร้าง End-to-End Data Pipelines ด้วย Airflow ตั้งแต่การอ่านข้อมูล ทำความสะอาด ปรับรูปแบบ และโหลดเข้า Data Lake/Data Warehouse แบบอัตโนมัติ",
]


def _require_gemini_key():
    if not GEMINI_API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY is not set in the Airflow environment. "
            "Add a free-tier Google AI Studio API key to 00-bootcamp-project/.env."
        )
    return GEMINI_API_KEY


def _gather_data():
    df = pd.DataFrame({"text": COURSE_DATA})
    output = f"{DAGS_FOLDER}/course-data.parquet"
    df.to_parquet(output, index=False)
    print(f"Wrote {len(df)} rows to {output}")


def _get_embeddings():
    df = pd.read_parquet(f"{DAGS_FOLDER}/course-data.parquet")
    genai_client = genai.Client(api_key=_require_gemini_key())

    def generate_embedding(text):
        result = genai_client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=text,
        )
        print(f"Embedded: {text[:80]}...")
        return result.embeddings[0].values

    df["embedding"] = df["text"].map(generate_embedding)
    output = f"{DAGS_FOLDER}/course-data-with-embeddings.parquet"
    df.to_parquet(output, index=False)
    print(f"Wrote embeddings to {output}")


def _load_data_to_bigquery():
    df = pd.read_parquet(f"{DAGS_FOLDER}/course-data-with-embeddings.parquet")

    with open(KEYFILE, encoding="utf-8") as handle:
        service_account_info = json.load(handle)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    load_job = bigquery_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {table_id}")


with DAG(
    dag_id="llm_course_data_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio", "week6", "RAG"],
):
    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=_gather_data,
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
