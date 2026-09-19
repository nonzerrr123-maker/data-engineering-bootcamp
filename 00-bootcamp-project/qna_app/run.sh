#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$HERE/.." && pwd)"

if [[ -f "$PROJECT_DIR/.env" ]]; then
  set -a
  source "$PROJECT_DIR/.env"
  set +a
fi

if [[ -z "\${GEMINI_API_KEY:-}" ]]; then
  echo "ERROR: GEMINI_API_KEY is missing."
  echo "Export a free-tier Google AI Studio key before starting the Q&A app."
  exit 2
fi

cd "$HERE"
poetry install --no-interaction
exec poetry run streamlit run app.py --server.address 0.0.0.0 --server.port 8501
