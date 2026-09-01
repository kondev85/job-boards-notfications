#!/usr/bin/env bash
set -euo pipefail

uv run test_job_boards.py
uv run test_postgres_persistence.py
uv run test_gemini_service.py
python3 -m compileall -q job_boards.py postgres_persistence.py \
  gemini_service.py test_job_boards.py test_postgres_persistence.py \
  test_gemini_service.py