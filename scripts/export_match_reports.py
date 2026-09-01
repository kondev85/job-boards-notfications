#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Export open matched roles for the default saved profile.

The generated files are intentionally kept out of version control:

    uv run scripts/export_match_reports.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
CSV_PATH = REPORTS_DIR / "matched_roles.csv"
JSON_PATH = REPORTS_DIR / "matched_roles.json"
DEFAULT_EMAIL = "infobettor@gmail.com"

FIELDS = (
    "rank",
    "match_id",
    "score",
    "match_status",
    "model_name",
    "model_version",
    "user_name",
    "user_email",
    "job_id",
    "ats",
    "external_id",
    "company",
    "title",
    "department",
    "team",
    "employment_type",
    "location",
    "is_remote",
    "workplace_type",
    "published_at",
    "job_url",
    "description",
    "match_created_at",
    "match_updated_at",
)


def _json_value(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _export() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")

    query = """
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    jm.score DESC,
                    j.published_at DESC NULLS LAST,
                    j.company,
                    j.title,
                    j.job_id
            ) AS rank,
            jm.match_id,
            jm.score,
            jm.match_status,
            jm.model_name,
            jm.model_version,
            u.name,
            u.email,
            j.job_id,
            j.ats,
            j.external_id,
            j.company,
            j.title,
            j.department,
            j.team,
            j.employment_type,
            j.location_raw,
            j.is_remote,
            j.workplace_type,
            j.published_at,
            j.job_url,
            j.description_text,
            jm.created_at,
            jm.updated_at
        FROM job_matches AS jm
        JOIN users AS u ON u.user_id = jm.user_id
        JOIN jobs AS j ON j.job_id = jm.job_id
        WHERE u.email = %s
          AND jm.match_status = 'matched'
          AND j.closed_at IS NULL
        ORDER BY
            jm.score DESC,
            j.published_at DESC NULLS LAST,
            j.company,
            j.title,
            j.job_id
    """

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(query, (DEFAULT_EMAIL,)).fetchall()

    records = []
    for row in rows:
        record = dict(zip(FIELDS, row))
        record["location"] = record.pop("location", None)
        records.append({key: _json_value(value) for key, value in record.items()})

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    JSON_PATH.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Exported {len(records)} matched roles for {DEFAULT_EMAIL}")
    print(f"CSV:  {CSV_PATH.relative_to(ROOT)}")
    print(f"JSON: {JSON_PATH.relative_to(ROOT)}")
    return len(records)


if __name__ == "__main__":
    raise SystemExit(0 if _export() >= 0 else 1)