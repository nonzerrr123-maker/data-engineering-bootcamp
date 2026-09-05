#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p dags config logs plugins tests pyspark spark-events

touch .env

if ! grep -q '^AIRFLOW_UID=' .env; then
  printf 'AIRFLOW_UID=%s\n' "$(id -u)" >> .env
fi

if ! grep -q '^FERNET_KEY=.' .env; then
  # Fernet keys are 32 random bytes encoded with URL-safe base64.
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
  echo "Airflow can still open and parse the DAGs, but GCS/BigQuery tasks need this local key."
fi

echo 'Starting Day 4 Airflow + Spark...'
docker compose down --remove-orphans
docker compose up --build -d --force-recreate

echo 'Waiting for Airflow API server...'
for _ in $(seq 1 60); do
  if docker compose exec -T airflow-apiserver curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker compose exec -T airflow-apiserver \
  python -c "import pyspark; assert pyspark.__version__ == '4.1.0'; print('PySpark', pyspark.__version__, 'OK')"

docker compose exec -T airflow-apiserver \
  python -c "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator; print('Spark provider OK')"

docker compose exec -T airflow-apiserver \
  python -c "from airflow.models import DagBag; b=DagBag(dag_folder='/opt/airflow/dags'); expected={'greenery_addresses_data_pipeline','greenery_order_items_data_pipeline','greenery_products_data_pipeline','greenery_promos_data_pipeline','greenery_events_data_pipeline','greenery_orders_data_pipeline','greenery_users_data_pipeline'}; missing=expected-set(b.dags); print('Day 4 DAGs:', len(expected)-len(missing), '/ 7'); assert not b.import_errors, b.import_errors; assert not missing, missing"

echo
echo 'Day 4 runtime checks passed.'
echo 'Open the forwarded Codespaces port 8080 to view Airflow.'
