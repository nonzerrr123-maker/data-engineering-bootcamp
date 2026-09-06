#!/usr/bin/env bash
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
KEY_DIR="$ROOT/00-bootcamp-project/pyspark"

cd "$HERE"

KEYFILE="$(find "$KEY_DIR" -maxdepth 1 -type f -name '*.json' -print -quit 2>/dev/null || true)"
if [[ -z "$KEYFILE" ]]; then
  echo "ERROR: ไม่พบ service-account JSON เดิมจาก Week 4 ใน $KEY_DIR"
  exit 2
fi

readarray -t GCP_INFO < <(python - "$KEYFILE" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    d = json.load(f)
print(d.get("project_id", ""))
print(d.get("client_email", ""))
PY
)
PROJECT_ID="${GCP_INFO[0]}"
CLIENT_EMAIL="${GCP_INFO[1]}"
if [[ -z "$PROJECT_ID" ]]; then
  echo "ERROR: service-account JSON ไม่มี project_id"
  exit 2
fi

echo "GCP project: $PROJECT_ID"
echo "Service account: $CLIENT_EMAIL"

# Prepare the local Airflow .env without exposing secrets to Git.
touch .env
if ! grep -q '^AIRFLOW_UID=' .env; then
  echo "AIRFLOW_UID=$(id -u)" >> .env
fi
if ! grep -q '^FERNET_KEY=' .env; then
  python - <<'PY' >> .env
from cryptography.fernet import Fernet
print("FERNET_KEY=" + Fernet.generate_key().decode())
PY
fi

# Week 4 and Week 5 both use host port 8080. Stop only the Week 4 containers;
# volumes/data are intentionally kept.
if [[ -f "$ROOT/00-bootcamp-project/docker-compose.yml" ]]; then
  echo "Stopping Week 4 Airflow temporarily to free port 8080..."
  (cd "$ROOT/00-bootcamp-project" && docker compose down >/dev/null 2>&1 || true)
fi

echo "Building and starting Week 5 Airflow + Astronomer Cosmos..."
docker compose up -d --build

# Wait for Airflow API server.
for i in $(seq 1 90); do
  if docker compose exec -T airflow-apiserver airflow version >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" -eq 90 ]]; then
    echo "ERROR: Airflow API server did not become ready"
    docker compose ps
    exit 3
  fi
  sleep 2
done

# Store the existing service-account JSON in the Airflow connection. Cosmos
# converts this google_cloud_platform connection into a temporary dbt profile.
EXTRA="$(python - "$KEYFILE" "$PROJECT_ID" <<'PY'
import json, sys
keyfile, project = sys.argv[1:3]
with open(keyfile, encoding="utf-8") as f:
    key = json.load(f)
print(json.dumps({
    "project": project,
    "dataset": "dbt_suntisuk",
    "keyfile_dict": key,
}, separators=(",", ":")))
PY
)"

docker compose exec -T airflow-apiserver airflow connections delete bigquery_dbt >/dev/null 2>&1 || true
docker compose exec -T airflow-apiserver airflow connections add bigquery_dbt \
  --conn-type google_cloud_platform \
  --conn-extra "$EXTRA" >/dev/null
unset EXTRA

# Reparse after the required connection exists.
docker compose restart airflow-dag-processor airflow-scheduler >/dev/null

echo "Waiting for Week 5 DAGs to register..."
FOUND=0
for i in $(seq 1 90); do
  OUT="$(docker compose exec -T airflow-apiserver airflow dags list 2>&1 || true)"
  if grep -q 'greenery_dbt_dag' <<<"$OUT" && grep -q 'greenery_dbt_docs' <<<"$OUT"; then
    FOUND=1
    break
  fi
  sleep 2
done

if [[ "$FOUND" -ne 1 ]]; then
  echo "ERROR: Week 5 DAG registration failed"
  echo "--- dags list ---"
  docker compose exec -T airflow-apiserver airflow dags list || true
  echo "--- import errors ---"
  docker compose exec -T airflow-apiserver airflow dags list-import-errors || true
  echo "--- dag processor logs ---"
  docker compose logs --no-color --tail=200 airflow-dag-processor || true
  exit 4
fi

echo
echo "WEEK 5 WORKSHOP AIRFLOW READY ✅"
echo "DAG: greenery_dbt_dag"
echo "DAG: greenery_dbt_docs"
echo "Open Codespaces port 8080 and sign in with airflow / airflow"
