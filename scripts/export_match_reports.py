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
RECOMMENDATION_CSV_PATH = REPORTS_DIR / "konstantin_recommendations.csv"
RECOMMENDATION_JSON_PATH = REPORTS_DIR / "konstantin_recommendations.json"
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
    "address",
    "is_remote",
    "workplace_type",
    "published_at",
    "job_url",
    "description",
    "match_created_at",
    "match_updated_at",
)

RECOMMENDATION_FIELDS = (
    "rank",
    "run_id",
    "candidate_floor",
    "match_id",
    "job_id",
    "ats",
    "external_id",
    "company",
    "title",
    "department",
    "team",
    "employment_type",
    "location",
    "address",
    "is_remote",
    "workplace_type",
    "published_at",
    "job_url",
    "deterministic_score",
    "batch_fit_score",
    "gemini_fit_score",
    "recommendation",
    "strengths",
    "concerns",
    "rationale",
    "description",
)


def _json_value(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _export(published_after: datetime | None = None) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")

    cutoff_clause = ""
    query_params: list[object] = [DEFAULT_EMAIL]
    if published_after is not None:
        cutoff_clause = " AND j.published_at >= %s"
        query_params.append(published_after)

    query = f"""
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
            j.address,
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
          {cutoff_clause}
        ORDER BY
            jm.score DESC,
            j.published_at DESC NULLS LAST,
            j.company,
            j.title,
            j.job_id
    """

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(query, query_params).fetchall()

    records = []
    for row in rows:
        record = dict(zip(FIELDS, row))
        record["location"] = record.pop("location", None)
        records.append({key: _json_value(value) for key, value in record.items()})

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_records = [
        record
        | {
            "address": (
                json.dumps(record["address"], ensure_ascii=False, sort_keys=True)
                if record["address"] is not None
                else ""
            )
        }
        for record in records
    ]
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(csv_records)
    JSON_PATH.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Exported {len(records)} matched roles for {DEFAULT_EMAIL}")
    print(f"CSV:  {CSV_PATH.relative_to(ROOT)}")
    print(f"JSON: {JSON_PATH.relative_to(ROOT)}")
    return len(records)


def _export_recommendations(
    published_after: datetime | None = None,
    *,
    top_n: int = 10,
) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")
    query = """
        WITH latest_run AS (
            SELECT run_id, candidate_floor
            FROM profile_recommendation_runs AS rr
            JOIN users AS u ON u.user_id = rr.user_id
            WHERE u.email = %s
              AND rr.published_after IS NOT DISTINCT FROM %s
            ORDER BY rr.created_at DESC
            LIMIT 1
        )
        SELECT
            r.final_rank,
            latest_run.run_id,
            latest_run.candidate_floor,
            jm.match_id,
            j.job_id,
            j.ats,
            j.external_id,
            j.company,
            j.title,
            j.department,
            j.team,
            j.employment_type,
            j.location_raw,
            j.address,
            j.is_remote,
            j.workplace_type,
            j.published_at,
            j.job_url,
            jm.score,
            r.batch_fit_score,
            r.final_fit_score,
            r.final_recommendation,
            r.final_strengths,
            r.final_concerns,
            r.final_rationale,
            j.description_text
        FROM latest_run
        JOIN job_profile_reviews AS r
          ON r.recommendation_run_id = latest_run.run_id
        JOIN job_matches AS jm
          ON jm.user_id = r.user_id AND jm.job_id = r.job_id
        JOIN jobs AS j ON j.job_id = r.job_id
        WHERE j.closed_at IS NULL
        ORDER BY r.final_rank
        LIMIT %s
    """
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(query, (DEFAULT_EMAIL, published_after, top_n)).fetchall()

    records = []
    for row in rows:
        record = dict(zip(RECOMMENDATION_FIELDS, row))
        records.append({key: _json_value(value) for key, value in record.items()})
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_records = []
    for record in records:
        csv_record = dict(record)
        for field in ("address", "strengths", "concerns"):
            value = csv_record[field]
            csv_record[field] = (
                json.dumps(value, ensure_ascii=False, sort_keys=True)
                if value is not None
                else ""
            )
        csv_records.append(csv_record)
    with RECOMMENDATION_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=RECOMMENDATION_FIELDS)
        writer.writeheader()
        writer.writerows(csv_records)
    RECOMMENDATION_JSON_PATH.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Exported {len(records)} recommendations for {DEFAULT_EMAIL}")
    print(f"CSV:  {RECOMMENDATION_CSV_PATH.relative_to(ROOT)}")
    print(f"JSON: {RECOMMENDATION_JSON_PATH.relative_to(ROOT)}")
    return len(records)


if __name__ == "__main__":
    raise SystemExit(0 if _export() >= 0 else 1)