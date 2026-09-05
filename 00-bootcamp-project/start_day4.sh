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

KEYFILE='pyspark/project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json'
if [[ ! -f "$KEYFILE" ]]; then
  echo "WARNING: GCP service-account key is not present at $KEYFILE"
  echo "Airflow can still load the DAGs, but GCS/BigQuery tasks need this local key."
fi

echo 'Resetting old Day 4 Airflow metadata...'
# This project is a local bootcamp environment. Removing the Postgres volume avoids
# stale serialized-DAG/import-error state left by previous Airflow images.
docker compose down -v --remove-orphans

echo 'Starting Day 4 Airflow + Spark...'
docker compose up --build -d --force-recreate

echo 'Waiting for Airflow API server...'
api_ok=0
for _ in $(seq 1 90); do
  if docker compose exec -T airflow-apiserver curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then
    api_ok=1
    break
  fi
  sleep 2
done

if [[ "$api_ok" -ne 1 ]]; then
  echo 'Airflow API server did not become healthy.'
  docker compose ps
  docker compose logs --no-color --tail=200 airflow-apiserver
  exit 1
fi

docker compose exec -T airflow-apiserver \
  python -c "import pyspark; assert pyspark.__version__ == '4.1.0'; print('PySpark', pyspark.__version__, 'OK')"

docker compose exec -T airflow-apiserver \
  python -c "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator; print('Spark provider OK')"

expected=(
  greenery_addresses_data_pipeline
  greenery_order_items_data_pipeline
  greenery_products_data_pipeline
  greenery_promos_data_pipeline
  greenery_events_data_pipeline
  greenery_orders_data_pipeline
  greenery_users_data_pipeline
)

echo 'Waiting for the DAG processor to register all seven DAGs...'
dags_ok=0
for _ in $(seq 1 90); do
  output="$(docker compose exec -T airflow-scheduler airflow dags list 2>&1 || true)"
  missing=0
  for dag in "${expected[@]}"; do
    if ! grep -q "$dag" <<<"$output"; then
      missing=$((missing + 1))
    fi
  done

  if [[ "$missing" -eq 0 ]]; then
    dags_ok=1
    break
  fi
  sleep 2
done

if [[ "$dags_ok" -ne 1 ]]; then
  echo 'Airflow did not register all seven Day 4 DAGs.'
  echo 'Current DAG list:'
  docker compose exec -T airflow-scheduler airflow dags list || true
  echo 'Import errors:'
  docker compose exec -T airflow-scheduler airflow dags list-import-errors || true
  echo 'DAG processor logs:'
  docker compose logs --no-color --tail=300 airflow-dag-processor
  exit 1
fi

echo 'Registered Day 4 DAGs:'
for dag in "${expected[@]}"; do
  echo "  - $dag"
done

echo
echo 'Day 4 runtime checks passed.'
echo 'Open the forwarded Codespaces port 8080 and refresh the Airflow Dags page.'
