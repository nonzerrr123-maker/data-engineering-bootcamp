#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p dags config logs plugins tests pyspark spark-events
touch .env

if ! grep -q '^AIRFLOW_UID=' .env; then
  printf 'AIRFLOW_UID=%s\n' "$(id -u)" >> .env
fi

if ! grep -q '^FERNET_KEY=.' .env; then
  FERNET_KEY="$(python -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
  if grep -q '^FERNET_KEY=' .env; then
    sed -i "s|^FERNET_KEY=.*|FERNET_KEY=${FERNET_KEY}|" .env
  else
    printf 'FERNET_KEY=%s\n' "$FERNET_KEY" >> .env
  fi
fi

# If the current shell has a Gemini key, sync it into the local ignored .env.
# This intentionally replaces an older/depleted key without ever printing it.
if [[ -n "${GEMINI_API_KEY:-}" ]]; then
  if grep -q '^GEMINI_API_KEY=' .env; then
    sed -i "s|^GEMINI_API_KEY=.*|GEMINI_API_KEY=${GEMINI_API_KEY}|" .env
  else
    printf 'GEMINI_API_KEY=%s\n' "$GEMINI_API_KEY" >> .env
  fi
fi

if ! grep -q '^GEMINI_API_KEY=.' .env; then
  echo "ERROR: GEMINI_API_KEY is missing."
  echo "Create a free-tier key in Google AI Studio, then run:"
  echo "  export GEMINI_API_KEY='...'"
  echo "  bash 00-bootcamp-project/start_week6_project.sh"
  exit 2
fi

KEYFILE='pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json'
if [[ ! -f "$KEYFILE" ]]; then
  echo "ERROR: GCP service-account key is missing at $KEYFILE"
  echo "Keep the JSON file local. Do not commit it."
  exit 2
fi

ROOT="$(cd .. && pwd)"
WEEK5_DIR="$ROOT/05-analytics-engineering/scheduling-dbt"
if [[ -f "$WEEK5_DIR/docker-compose.yml" ]]; then
  echo 'Stopping Week 5 Airflow to free host port 8080...'
  (cd "$WEEK5_DIR" && docker compose down >/dev/null 2>&1 || true)
fi

echo 'Resetting local Airflow metadata so the Week 6 project DAG is loaded cleanly...'
docker compose down -v --remove-orphans

echo 'Building and starting Airflow + Spark for Week 6...'
docker compose up --build -d --force-recreate

echo 'Waiting for Airflow API server...'
api_ok=0
for _ in $(seq 1 120); do
  if docker compose exec -T airflow-apiserver curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then
    api_ok=1
    break
  fi
  sleep 2
done

if [[ "$api_ok" -ne 1 ]]; then
  echo 'ERROR: Airflow API server did not become healthy.'
  docker compose ps
  docker compose logs --no-color --tail=250 airflow-apiserver
  exit 1
fi

echo 'Checking Week 6 project packages inside the Airflow image...'
docker compose exec -T airflow-apiserver   python -c "from google import genai; import pandas_gbq, pyarrow; print('Week 6 project packages OK')"

echo 'Waiting for greenery_llm_rag_pipeline to register...'
dag_ok=0
for _ in $(seq 1 90); do
  output="$(docker compose exec -T airflow-scheduler airflow dags list 2>&1 || true)"
  if grep -q 'greenery_llm_rag_pipeline' <<<"$output"; then
    dag_ok=1
    break
  fi
  sleep 2
done

if [[ "$dag_ok" -ne 1 ]]; then
  echo 'ERROR: greenery_llm_rag_pipeline was not registered.'
  docker compose exec -T airflow-scheduler airflow dags list || true
  docker compose exec -T airflow-scheduler airflow dags list-import-errors || true
  docker compose logs --no-color --tail=300 airflow-dag-processor
  exit 1
fi

echo 'Checking Gemini free-tier credentials with one small embedding request...'
if ! docker compose exec -T airflow-apiserver python - <<'PY'
import os
from google import genai

key = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=key)
result = client.models.embed_content(
    model=os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2"),
    contents="Week 6 Greenery project preflight",
)
values = result.embeddings[0].values
assert values, "Gemini returned an empty embedding"
print(f"Gemini embedding preflight OK (dimension={len(values)})")
PY
then
  echo
  echo "ERROR: Gemini preflight failed."
  echo "If the message says RESOURCE_EXHAUSTED / prepayment credits depleted,"
  echo "replace GEMINI_API_KEY with a free-tier Google AI Studio project/key."
  exit 1
fi

echo 'Triggering the Week 6 Greenery RAG pipeline...'
trigger_output="$(docker compose exec -T airflow-scheduler airflow dags trigger greenery_llm_rag_pipeline 2>&1)"
echo "$trigger_output"

echo 'Waiting for the triggered DAG run to finish...'
run_state=""
for _ in $(seq 1 180); do
  runs="$(docker compose exec -T airflow-scheduler airflow dags list-runs --dag-id greenery_llm_rag_pipeline --output json 2>/dev/null || true)"
  run_state="$(python -c 'import json,sys
try:
    data=json.load(sys.stdin)
    print(data[0].get("state","") if data else "")
except Exception:
    print("")' <<<"$runs")"
  case "$run_state" in
    success)
      break
      ;;
    failed)
      echo 'ERROR: Week 6 project DAG failed.'
      docker compose exec -T airflow-scheduler airflow dags list-runs --dag-id greenery_llm_rag_pipeline || true
      exit 1
      ;;
  esac
  sleep 2
done

if [[ "$run_state" != "success" ]]; then
  echo "ERROR: Timed out waiting for greenery_llm_rag_pipeline (last state: ${run_state:-unknown})."
  exit 1
fi

echo 'Verifying Greenery RAG context in BigQuery...'
docker compose exec -T airflow-apiserver python - <<'PY'
import json
from google.cloud import bigquery
from google.oauth2 import service_account

project = "project-b9bafacf-46f9-43ef-bcc"
dataset = "deb_bootcamp"
table = "greenery_rag_context"
keyfile = "/opt/spark/pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json"

with open(keyfile, encoding="utf-8") as f:
    info = json.load(f)
credentials = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(project=project, credentials=credentials)

rows = list(client.query(
    f"SELECT COUNT(*) AS n FROM \`{project}.{dataset}.{table}\`"
).result())
count = int(rows[0].n)
assert count >= 2, f"Expected at least 2 rows in {table}, got {count}"
print(f"BigQuery verification OK: {project}.{dataset}.{table} has {count} rows")
PY

echo
echo 'WEEK 6 BOOTCAMP PROJECT RUNTIME PASSED ✅'
echo 'Airflow DAG: greenery_llm_rag_pipeline'
echo 'BigQuery table: project-b9bafacf-46f9-43ef-bcc.deb_bootcamp.greenery_rag_context'
echo 'Open Codespaces port 8080 only if you want to visually inspect the successful DAG.'
