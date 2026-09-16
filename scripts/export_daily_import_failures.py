#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Export the failed boards from a PostgreSQL daily import run.

The database remains the source of truth. These ignored workspace reports are
operator-friendly snapshots: the latest report is easy to open, and a
run-specific copy preserves the exact failure list for later cleanup.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
FIELDS = (
    "import_run_id",
    "run_status",
    "published_after",
    "started_at",
    "updated_at",
    "board_count",
    "failed_board_count",
    "position",
    "board_id",
    "ats",
    "slug",
    "company",
    "source_url",
    "active",
    "board_status",
    "attempts",
    "jobs_fetched",
    "jobs_inserted",
    "jobs_updated",
    "error_type",
    "error_message",
    "retry_after",
    "completed_at",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _latest_run_id(conn: psycopg.Connection) -> int | None:
    row = conn.execute(
        "SELECT import_run_id FROM daily_import_runs "
        "ORDER BY started_at DESC, import_run_id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _failed_rows(
    conn: psycopg.Connection,
    import_run_id: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            r.import_run_id,
            r.status,
            r.published_after,
            r.started_at,
            r.updated_at,
            r.board_count,
            r.failed_board_count,
            rb.position,
            rb.board_id,
            b.ats,
            b.slug,
            c.display_name,
            b.source_url,
            b.active,
            rb.status,
            rb.attempts,
            rb.jobs_fetched,
            rb.jobs_inserted,
            rb.jobs_updated,
            rb.error_type,
            rb.error_message,
            rb.retry_after,
            rb.completed_at
        FROM daily_import_runs AS r
        JOIN daily_import_run_boards AS rb
          ON rb.import_run_id = r.import_run_id
        JOIN job_boards AS b
          ON b.board_id = rb.board_id
        JOIN companies AS c
          ON c.company_id = b.company_id
        WHERE r.import_run_id = %s
          AND rb.status = 'failed'
        ORDER BY b.ats, b.slug
        """,
        (import_run_id,),
    ).fetchall()
    return [
        {
            key: _json_value(value)
            for key, value in zip(FIELDS, row)
        }
        for row in rows
    ]


def _write_report(path: Path, records: list[dict[str, Any]]) -> None:
    if path.suffix == ".csv":
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(records)
    else:
        path.write_text(
            json.dumps(records, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def export_daily_import_failures(
    dsn: str,
    import_run_id: int | None = None,
    *,
    output_dir: Path = REPORTS_DIR,
) -> dict[str, Path | int]:
    """Write latest and run-specific failed-board reports."""
    with psycopg.connect(dsn) as conn:
        selected_run_id = import_run_id or _latest_run_id(conn)
        if selected_run_id is None:
            raise ValueError("no daily import runs exist")
        records = _failed_rows(conn, selected_run_id)

    output_dir.mkdir(parents=True, exist_ok=True)
    latest_csv = output_dir / "daily_import_failures.csv"
    latest_json = output_dir / "daily_import_failures.json"
    run_csv = output_dir / f"daily_import_failures_run_{selected_run_id}.csv"
    run_json = output_dir / f"daily_import_failures_run_{selected_run_id}.json"
    for path in (latest_csv, latest_json, run_csv, run_json):
        _write_report(path, records)
    return {
        "csv": latest_csv,
        "json": latest_json,
        "run_csv": run_csv,
        "run_json": run_json,
        "count": len(records),
        "run_id": selected_run_id,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export failed boards from a daily PostgreSQL import run."
    )
    parser.add_argument("--run-id", type=int, help="run to export; defaults to latest")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPORTS_DIR,
        help="directory for the ignored CSV and JSON reports",
    )
    args = parser.parse_args()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")
    result = export_daily_import_failures(
        dsn,
        args.run_id,
        output_dir=args.output_dir,
    )
    print(
        f"Exported {result['count']} failed boards for daily import run "
        f"{result['run_id']}"
    )
    print(f"CSV:  {result['csv']}")
    print(f"JSON: {result['json']}")
    print(f"Run CSV:  {result['run_csv']}")
    print(f"Run JSON: {result['run_json']}")


if __name__ == "__main__":
    main()