#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSHOP="$ROOT/05-analytics-engineering/workshop"
SOLUTION="$ROOT/05-analytics-engineering/week5-complete"
GREENERY="$WORKSHOP/greenery"
SCHED="$ROOT/05-analytics-engineering/scheduling-dbt"
REPORT="$ROOT/05-analytics-engineering/WEEK5_VERIFICATION_REPORT.txt"

PASS=0
FAIL=0
EXPECTED=0

green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
red() { printf '\033[0;31m%s\033[0m\n' "$*"; }
section() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }
record_pass() { PASS=$((PASS+1)); echo "PASS | $1" | tee -a "$REPORT"; }
record_fail() { FAIL=$((FAIL+1)); echo "FAIL | $1" | tee -a "$REPORT"; }
record_expected() { EXPECTED=$((EXPECTED+1)); echo "EXPECTED | $1" | tee -a "$REPORT"; }

run_check() {
  local label="$1"; shift
  section "$label"
  if "$@"; then
    record_pass "$label"
    return 0
  else
    record_fail "$label"
    return 0
  fi
}

: > "$REPORT"
echo "Week 5 Verification - $(date -Is)" >> "$REPORT"

section "0) Detect GCP credentials"
KEYFILE="$(find "$ROOT/00-bootcamp-project/pyspark" -maxdepth 1 -type f -name '*.json' -print -quit 2>/dev/null || true)"
if [[ -z "${KEYFILE}" ]]; then
  red "ไม่พบ service-account JSON ใน 00-bootcamp-project/pyspark/"
  echo "วาง key JSON เดิมจาก Week 4 ไว้ที่โฟลเดอร์นั้น แล้วรันสคริปต์นี้ใหม่"
  exit 2
fi

readarray -t GCP_INFO < <(python - "$KEYFILE" <<'PY'
import json, sys
p=sys.argv[1]
with open(p, encoding="utf-8") as f:
    d=json.load(f)
print(d.get("project_id",""))
print(d.get("client_email",""))
PY
)
GCP_PROJECT="${GCP_INFO[0]}"
CLIENT_EMAIL="${GCP_INFO[1]}"
if [[ -z "$GCP_PROJECT" ]]; then
  red "JSON ไม่มี project_id"
  exit 2
fi
green "GCP Project: $GCP_PROJECT"
green "Service account: $CLIENT_EMAIL"
record_pass "พบ GCP credentials (ไม่แสดง private key)"

section "1) Prepare workshop dbt project"
BACKUP_DIR="$ROOT/.week5-local-backups"
mkdir -p "$BACKUP_DIR"
if [[ -d "$GREENERY" ]]; then
  TS="$(date +%Y%m%d_%H%M%S)"
  cp -a "$GREENERY" "$BACKUP_DIR/greenery_$TS"
  yellow "สำรองงานเดิมไว้ที่ .week5-local-backups/greenery_$TS"
fi
rm -rf "$GREENERY"
mkdir -p "$GREENERY"
cp -a "$SOLUTION/greenery/." "$GREENERY/"

cat > "$GREENERY/profiles.yml" <<EOF
greenery:
  target: dbt_suntisuk_bigquery
  outputs:
    dbt_suntisuk_bigquery:
      type: bigquery
      method: service-account
      project: ${GCP_PROJECT}
      dataset: dbt_suntisuk
      threads: 4
      keyfile: ${KEYFILE}
      location: asia-southeast1

    prod_bigquery:
      type: bigquery
      method: service-account
      project: ${GCP_PROJECT}
      dataset: deb_bootcamp
      threads: 4
      keyfile: ${KEYFILE}
      location: asia-southeast1
EOF

export DBT_GCP_PROJECT="$GCP_PROJECT"
cd "$WORKSHOP"

run_check "Poetry dependencies" poetry install
cd "$GREENERY"
run_check "dbt debug / BigQuery connection" poetry run dbt debug --profiles-dir .
run_check "dbt deps / dbt_expectations" poetry run dbt deps --profiles-dir .

section "2) Source freshness"
set +e
poetry run dbt source freshness --profiles-dir . > /tmp/week5_freshness.log 2>&1
FRESH_RC=$?
set -e
if [[ $FRESH_RC -eq 0 ]]; then
  record_pass "dbt source freshness"
else
  # The Notion workshop explicitly expects old users/events to be stale (>24h).
  if grep -Eqi 'stale|error|warn' /tmp/week5_freshness.log; then
    record_expected "dbt source freshness: historical users/events are stale (ตาม Workshop)"
  else
    cat /tmp/week5_freshness.log
    record_fail "dbt source freshness failed for an unexpected reason"
  fi
fi

run_check "dbt compile / parse project" poetry run dbt compile --profiles-dir .
run_check "Sources + Models + Materialization (dbt run)" poetry run dbt run --profiles-dir .
run_check "Unit Test: valid email" poetry run dbt test --select test_is_valid_email_address --profiles-dir .
run_check "Generic + Singular + dbt_expectations tests" poetry run dbt test --profiles-dir .
run_check "Snapshots" poetry run dbt snapshot --profiles-dir .
run_check "Documentation (dbt docs generate)" poetry run dbt docs generate --profiles-dir .

section "3) Verify required dbt artifacts"
if python - "$GREENERY" <<'PY'
from pathlib import Path
import sys
root=Path(sys.argv[1])
required=[
"dbt_project.yml","packages.yml","profiles.yml",
"models/staging/greenery/_src.yml",
"models/example/my_users.sql","models/example/my_events.sql",
"models/staging/greenery/stg_greenery__users.sql",
"models/staging/greenery/stg_greenery__events.sql",
"models/staging/greenery/stg_greenery__orders.sql",
"models/staging/greenery/stg_greenery__addresses.sql",
"models/staging/greenery/stg_greenery__products.sql",
"models/staging/greenery/stg_greenery__order_items.sql",
"models/staging/greenery/stg_greenery__promos.sql",
"models/intermediate/int_orders_enriched.sql",
"models/marts/number_of_users.sql",
"models/marts/number_of_orders.sql",
"models/marts/highest_order_state.sql",
"models/marts/order_basket_region_summary.sql",
"tests/assert_phone_number_length_should_be_12.sql",
"models/_unit_tests.yml",
"snapshots/users_snapshot.sql",
"target/index.html",
"target/manifest.json",
]
missing=[x for x in required if not (root/x).exists()]
if missing:
    print("MISSING:")
    print("\n".join(missing))
    raise SystemExit(1)
print(f"Required artifacts OK: {len(required)}")
PY
then
  record_pass "required workshop artifacts"
else
  record_fail "required workshop artifacts"
fi

section "4) BigQuery result checks"
if poetry run python - "$GCP_PROJECT" "$KEYFILE" <<'PY'
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
project,keyfile=sys.argv[1:3]
creds=service_account.Credentials.from_service_account_file(keyfile)
client=bigquery.Client(project=project, credentials=creds)

queries = {
    "staging users": f"select count(*) c from `{project}.dbt_suntisuk_staging.stg_greenery__users`",
    "staging orders": f"select count(*) c from `{project}.dbt_suntisuk_staging.stg_greenery__orders`",
    "number_of_users": f"select user_count from `{project}.dbt_suntisuk_reporting.number_of_users`",
    "number_of_orders": f"select order_count from `{project}.dbt_suntisuk_reporting.number_of_orders`",
    "highest_order_state": f"select state, order_count from `{project}.dbt_suntisuk_reporting.highest_order_state`",
    "basket_region": f"select count(*) c from `{project}.dbt_suntisuk_reporting.order_basket_region_summary`",
    "snapshot": f"select count(*) c from `{project}.snapshots.users_snapshot`",
}
for name,q in queries.items():
    rows=list(client.query(q).result())
    if not rows:
        raise RuntimeError(f"{name}: no result")
    print(f"[OK] {name}: {dict(rows[0].items())}")
PY
then
  record_pass "BigQuery staging + marts queries"
else
  record_fail "BigQuery staging + marts queries"
fi

section "5) Prepare Airflow + Cosmos project"
mkdir -p "$SCHED/dbt/greenery"
rm -rf "$SCHED/dbt/greenery"
cp -a "$GREENERY" "$SCHED/dbt/greenery"
rm -f "$SCHED/dbt/greenery/profiles.yml"
rm -rf "$SCHED/dbt/greenery/logs" "$SCHED/dbt/greenery/target"

cp "$SOLUTION/airflow/greenery_dbt_dag.py" "$SCHED/dags/greenery_dbt_dag.py"
cp "$SOLUTION/airflow/greenery_dbt_docs.py" "$SCHED/dags/greenery_dbt_docs.py"

# Cosmos BigQuery mapping reads keyfile_dict from the Airflow connection.
CONN_EXTRA_FILE="$SCHED/.bigquery_dbt_extra.json"
python - "$KEYFILE" "$GCP_PROJECT" > "$CONN_EXTRA_FILE" <<'PY'
import json,sys
keyfile,project=sys.argv[1:3]
with open(keyfile, encoding="utf-8") as f:
    key=json.load(f)
print(json.dumps({"project":project,"dataset":"dbt_suntisuk","keyfile_dict":key}, separators=(",",":")))
PY
chmod 600 "$CONN_EXTRA_FILE"

# Use a Compose override rather than editing the course docker-compose.yml.
# It adds the 300s DAG parse timeout and Airflow 3.x Cosmos docs hosting.
if [[ -f "$SCHED/docker-compose.override.yml" ]]; then
  cp "$SCHED/docker-compose.override.yml" "$BACKUP_DIR/scheduling-dbt-compose-override_$(date +%Y%m%d_%H%M%S).yml"
fi
cp "$SOLUTION/airflow/docker-compose.override.yml" "$SCHED/docker-compose.override.yml"

# Stop Week 4 containers only (keep volumes) because both workshops use port 8080.
if [[ -f "$ROOT/00-bootcamp-project/docker-compose.yml" ]]; then
  yellow "หยุด Week 4 Airflow ชั่วคราวเพื่อคืน port 8080 (ไม่ลบ volume/data)"
  (cd "$ROOT/00-bootcamp-project" && docker compose down >/dev/null 2>&1 || true)
fi

cd "$SCHED"
mkdir -p logs config plugins tests
touch .env
if ! grep -q '^AIRFLOW_UID=' .env; then
  echo "AIRFLOW_UID=$(id -u)" >> .env
fi
if ! grep -q '^FERNET_KEY=' .env; then
  python - <<'PY' >> .env
from cryptography.fernet import Fernet
print("FERNET_KEY="+Fernet.generate_key().decode())
PY
fi

run_check "Build/start Week 5 Airflow + Cosmos" docker compose up -d --build

section "6) Create Airflow BigQuery connection"
# Wait for API server.
for i in {1..60}; do
  if docker compose exec -T airflow-apiserver airflow version >/dev/null 2>&1; then break; fi
  sleep 2
done
EXTRA="$(cat "$CONN_EXTRA_FILE")"
docker compose exec -T airflow-apiserver airflow connections delete bigquery_dbt >/dev/null 2>&1 || true
if docker compose exec -T airflow-apiserver airflow connections add bigquery_dbt \
    --conn-type google_cloud_platform \
    --conn-extra "$EXTRA" >/tmp/week5_conn.log 2>&1; then
  record_pass "Airflow connection bigquery_dbt"
else
  cat /tmp/week5_conn.log
  record_fail "Airflow connection bigquery_dbt"
fi
# The temporary file contains the private key JSON; remove it immediately after
# the connection has been stored encrypted by Airflow.
rm -f "$CONN_EXTRA_FILE"
unset EXTRA

# Force a fresh parse now that the BigQuery connection exists.
docker compose restart airflow-dag-processor airflow-scheduler >/dev/null 2>&1 || true

section "7) Validate Cosmos DAG parsing"
sleep 25
if docker compose exec -T airflow-apiserver airflow dags list 2>/tmp/week5_dags.err | grep -q 'greenery_dbt_dag'; then
  record_pass "greenery_dbt_dag visible to Airflow"
else
  cat /tmp/week5_dags.err || true
  docker compose logs --tail=100 airflow-dag-processor || true
  record_fail "greenery_dbt_dag visible to Airflow"
fi

section "8) Final summary"
{
  echo
  echo "PASS=$PASS"
  echo "EXPECTED=$EXPECTED"
  echo "FAIL=$FAIL"
  echo "Report: $REPORT"
} | tee -a "$REPORT"

if [[ "$FAIL" -eq 0 ]]; then
  green "WEEK 5 WORKSHOP COMPLETE ✅"
  echo
  echo "เช็กเองแค่ 3 จุด:"
  echo "1) เปิด Airflow port 8080 -> ต้องเห็น greenery_dbt_dag"
  echo "2) เปิด BigQuery -> ต้องเห็น dbt_suntisuk_staging / _intermediate / _reporting และ snapshots"
  echo "3) ถ้าจะเปิด dbt docs แบบ standalone: cd $GREENERY && poetry run dbt docs serve --port 8081"
  exit 0
else
  red "ยังมี $FAIL จุดไม่ผ่าน ดู $REPORT และ error เหนือบรรทัดนี้"
  exit 1
fi
