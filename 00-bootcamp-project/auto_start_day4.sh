#!/usr/bin/env bash
set -u

PROJECT_DIR="/workspaces/data-engineering-bootcamp/00-bootcamp-project"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# Start in the background so Codespaces can finish opening immediately.
(
  cd "$PROJECT_DIR"

  # Wait for Docker to become available inside the Codespace.
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  # If Airflow is already healthy, do nothing.
  if docker compose exec -T airflow-apiserver \
      curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then
    echo "Day 4 Airflow is already running."
    exit 0
  fi

  bash "$PROJECT_DIR/start_day4.sh"
) >"$LOG_DIR/auto_start_day4.log" 2>&1 &

exit 0
