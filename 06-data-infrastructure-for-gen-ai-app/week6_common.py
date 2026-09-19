import json
import os
from pathlib import Path

from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-b9bafacf-46f9-43ef-bcc")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "deb_bootcamp")
SAMPLE_TABLE_ID = os.getenv("WEEK6_SAMPLE_TABLE_ID", "my_embeddings")
COURSE_TABLE_ID = os.getenv("WEEK6_COURSE_TABLE_ID", "course_embeddings")
GENERATION_MODEL = os.getenv("GEMINI_GENERATION_MODEL", "gemini-2.5-flash-lite")
EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001")
DEFAULT_KEYFILE = (
    REPO_ROOT
    / "00-bootcamp-project"
    / "pyspark"
    / "project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json"
)
KEYFILE = Path(os.getenv("GCP_KEYFILE", str(DEFAULT_KEYFILE)))


def require_gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Create a free-tier API key in Google AI Studio "
            "and export GEMINI_API_KEY before running this workshop."
        )
    return api_key


def get_genai_client():
    return genai.Client(api_key=require_gemini_api_key())


def get_embedding(client, text: str, model: str = EMBEDDING_MODEL):
    result = client.models.embed_content(model=model, contents=text)
    return result.embeddings[0]


def ask_gemini(
    client,
    prompt: str,
    model: str = GENERATION_MODEL,
    system_instruction=None,
):
    kwargs = {"model": model, "contents": prompt}
    if system_instruction:
        from google.genai import types

        kwargs["config"] = types.GenerateContentConfig(
            system_instruction=list(system_instruction)
        )
    response = client.models.generate_content(**kwargs)
    return response.text


def get_bigquery_client(
    project_id: str = GCP_PROJECT_ID,
    keyfile: Path | str = KEYFILE,
):
    keyfile = Path(keyfile)
    if not keyfile.exists():
        raise FileNotFoundError(
            f"GCP service-account key not found: {keyfile}. "
            "Keep the JSON file local; do not commit it."
        )
    with keyfile.open(encoding="utf-8") as handle:
        service_account_info = json.load(handle)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )
    return bigquery.Client(project=project_id, credentials=credentials)


def full_table_id(table_id: str) -> str:
    return f"{GCP_PROJECT_ID}.{DATASET_ID}.{table_id}"
