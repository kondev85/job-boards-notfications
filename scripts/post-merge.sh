#!/usr/bin/env bash
set -euo pipefail

# Development schema changes are applied here, after a merge. Production schema
# changes are applied by the Replit Publish flow; the web process never runs DDL.
if [[ -n "${DATABASE_URL:-}" ]]; then
  if command -v psql >/dev/null 2>&1; then
    psql "$DATABASE_URL" --set ON_ERROR_STOP=1 --file migrations/001_authenticated_app.sql
  else
    echo "DATABASE_URL is set but psql is unavailable; skipped development schema setup" >&2
  fi
else
  echo "DATABASE_URL is not set; skipped development schema setup"
fi

uv run test_job_boards.py
uv run test_postgres_persistence.py
uv run test_gemini_service.py
python3 -m compileall -q job_boards.py postgres_persistence.py \
  gemini_service.py test_job_boards.py test_postgres_persistence.py \
  test_gemini_service.py