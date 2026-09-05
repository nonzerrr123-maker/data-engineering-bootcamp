#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/workspaces/data-engineering-bootcamp"
cd "$REPO_ROOT"

STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_DIR="/tmp/day4-untracked-backup-$STAMP"
mkdir -p "$BACKUP_DIR"

FILES=(
  "00-bootcamp-project/.env.spark"
  "00-bootcamp-project/docker-compose.yml"
  "00-bootcamp-project/docker/Dockerfile-airflow"
  "00-bootcamp-project/docker/Dockerfile-spark"
  "00-bootcamp-project/docker/conf/metrics.properties"
  "00-bootcamp-project/docker/conf/spark-defaults.conf"
  "00-bootcamp-project/docker/entrypoint.sh"
  "00-bootcamp-project/docker/jars/gcs-connector-hadoop3-latest.jar"
)

for f in "${FILES[@]}"; do
  if [[ -e "$f" ]]; then
    mkdir -p "$BACKUP_DIR/$(dirname "$f")"
    mv "$f" "$BACKUP_DIR/$f"
  fi
done

echo "Backed up conflicting local files to: $BACKUP_DIR"

git pull --ff-only

bash 00-bootcamp-project/start_day4.sh
