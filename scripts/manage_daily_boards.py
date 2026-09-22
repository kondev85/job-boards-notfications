#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Inspect failed daily boards and explicitly deactivate board registry rows."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg

# Running a script by path sets sys.path[0] to scripts/, not the project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import job_boards


def _latest_run_id(conn: psycopg.Connection) -> int | None:
    row = conn.execute(
        "SELECT import_run_id FROM daily_import_runs "
        "ORDER BY started_at DESC, import_run_id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _parse_board(value: str) -> tuple[str, str]:
    ats, separator, slug = value.partition(":")
    if not separator or ats not in job_boards.SOURCES or not slug.strip():
        raise SystemExit(
            f"board must use ATS:SLUG with ATS one of {', '.join(job_boards.SOURCES)}"
        )
    return ats, slug.strip()


def _list_failures(conn: psycopg.Connection, import_run_id: int | None) -> None:
    selected_run_id = import_run_id or _latest_run_id(conn)
    if selected_run_id is None:
        print("No daily import runs found.")
        return
    rows = conn.execute(
        """
        SELECT b.ats, b.slug, b.active, rb.attempts, rb.error_type,
               rb.error_message, rb.completed_at
        FROM daily_import_run_boards AS rb
        JOIN job_boards AS b ON b.board_id = rb.board_id
        WHERE rb.import_run_id = %s AND rb.status = 'failed'
        ORDER BY b.ats, b.slug
        """,
        (selected_run_id,),
    ).fetchall()
    print(f"Failed boards for daily import run {selected_run_id}: {len(rows)}")
    for ats, slug, active, attempts, error_type, message, completed_at in rows:
        print(
            f"{ats}:{slug} | active={active} | attempts={attempts} | "
            f"{error_type or 'unknown'} | {completed_at} | {message or ''}"
        )


def _deactivate(conn: psycopg.Connection, board_values: list[str]) -> None:
    for value in board_values:
        ats, slug = _parse_board(value)
        with conn.transaction():
            row = conn.execute(
                """
                UPDATE job_boards
                SET active = FALSE
                WHERE ats = %s AND slug = %s
                RETURNING board_id
                """,
                (ats, slug),
            ).fetchone()
        if row:
            print(f"Deactivated {ats}:{slug} (board_id={row[0]})")
        else:
            print(f"Board not found: {ats}:{slug}")


def _deactivate_failures(
    conn: psycopg.Connection,
    import_run_id: int,
    error_type: str,
    min_attempts: int,
) -> None:
    with conn.transaction():
        rows = conn.execute(
            """
            UPDATE job_boards AS b
            SET active = FALSE
            FROM daily_import_run_boards AS rb
            WHERE rb.import_run_id = %s
              AND rb.board_id = b.board_id
              AND rb.status = 'failed'
              AND rb.error_type = %s
              AND rb.attempts >= %s
              AND b.active IS TRUE
            RETURNING b.ats, b.slug
            """,
            (import_run_id, error_type, min_attempts),
        ).fetchall()
    print(
        f"Deactivated {len(rows)} {error_type} boards from run "
        f"{import_run_id} after at least {min_attempts} attempts."
    )
    for ats, slug in rows:
        print(f"{ats}:{slug}")


def _repeated_failures(
    conn: psycopg.Connection,
    first_run_id: int,
    second_run_id: int,
) -> list[dict[str, object]]:
    """Return boards that failed in both selected runs."""
    rows = conn.execute(
        """
        SELECT b.board_id, b.ats, b.slug, b.active,
               rb.import_run_id, rb.error_type, rb.error_message
        FROM daily_import_run_boards AS rb
        JOIN job_boards AS b ON b.board_id = rb.board_id
        WHERE rb.import_run_id IN (%s, %s)
          AND rb.status = 'failed'
        ORDER BY b.ats, b.slug, rb.import_run_id
        """,
        (first_run_id, second_run_id),
    ).fetchall()
    by_board: dict[int, dict[str, object]] = {}
    for board_id, ats, slug, active, run_id, error_type, error_message in rows:
        record = by_board.setdefault(
            board_id,
            {
                "board_id": board_id,
                "ats": ats,
                "slug": slug,
                "active": active,
                "runs": {},
            },
        )
        record["runs"][run_id] = {
            "error_type": error_type,
            "error_message": error_message,
        }
    return [
        record
        for record in by_board.values()
        if len(record["runs"]) == 2
    ]


def _deactivate_repeated_failures(
    conn: psycopg.Connection,
    repeated: list[dict[str, object]],
) -> int:
    board_ids = [int(record["board_id"]) for record in repeated]
    if not board_ids:
        return 0
    with conn.transaction():
        rows = conn.execute(
            """
            UPDATE job_boards
            SET active = FALSE
            WHERE board_id = ANY(%s)
              AND active IS TRUE
            RETURNING board_id, ats, slug
            """,
            (board_ids,),
        ).fetchall()
    for board_id, ats, slug in rows:
        print(f"Deactivated {ats}:{slug} (board_id={board_id})")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List failed daily boards or deactivate exact board keys."
    )
    parser.add_argument(
        "--list-failures",
        action="store_true",
        help="list failed boards from the latest run",
    )
    parser.add_argument("--run-id", type=int, help="run to inspect with --list-failures")
    parser.add_argument(
        "--deactivate",
        action="append",
        metavar="ATS:SLUG",
        help="deactivate one exact PostgreSQL board; repeatable",
    )
    parser.add_argument(
        "--deactivate-failures",
        action="store_true",
        help="deactivate matching failed boards from one specified run",
    )
    parser.add_argument(
        "--error-type",
        default="NotFound",
        help="error type for --deactivate-failures (default: NotFound)",
    )
    parser.add_argument(
        "--min-attempts",
        type=int,
        default=2,
        help="minimum recorded attempts for --deactivate-failures (default: 2)",
    )
    parser.add_argument(
        "--compare-runs",
        nargs=2,
        type=int,
        metavar=("RUN_A", "RUN_B"),
        help="compare failed boards from two daily import runs",
    )
    parser.add_argument(
        "--deactivate-repeated-failures",
        action="store_true",
        help="deactivate boards failed in both runs selected by --compare-runs",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply repeated-failure deactivation; otherwise only preview",
    )
    args = parser.parse_args()
    if not (
        args.list_failures
        or args.deactivate
        or args.deactivate_failures
        or args.compare_runs
    ):
        parser.error(
            "choose --list-failures, --deactivate, --deactivate-failures, "
            "or --compare-runs"
        )
    if args.run_id is not None and not (
        args.list_failures or args.deactivate_failures
    ):
        parser.error("--run-id requires --list-failures or --deactivate-failures")
    if args.deactivate_failures and args.run_id is None:
        parser.error("--deactivate-failures requires --run-id")
    if args.deactivate_repeated_failures and not args.compare_runs:
        parser.error("--deactivate-repeated-failures requires --compare-runs")
    if args.apply and not args.deactivate_repeated_failures:
        parser.error("--apply requires --deactivate-repeated-failures")
    if args.min_attempts < 1:
        parser.error("--min-attempts must be at least 1")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")
    with psycopg.connect(dsn) as conn:
        if args.list_failures:
            _list_failures(conn, args.run_id)
        if args.deactivate:
            _deactivate(conn, args.deactivate)
        if args.deactivate_failures:
            _deactivate_failures(
                conn,
                args.run_id,
                args.error_type,
                args.min_attempts,
            )
        if args.compare_runs:
            first_run_id, second_run_id = args.compare_runs
            if first_run_id == second_run_id:
                parser.error("--compare-runs requires two different run IDs")
            repeated = _repeated_failures(conn, first_run_id, second_run_id)
            print(
                f"Boards failed in both daily import runs "
                f"{first_run_id} and {second_run_id}: {len(repeated)}"
            )
            for record in repeated:
                runs = record["runs"]
                print(
                    f"{record['ats']}:{record['slug']} | "
                    f"active={record['active']} | board_id={record['board_id']} | "
                    f"run {first_run_id}: "
                    f"{runs[first_run_id]['error_type'] or 'unknown'} | "
                    f"run {second_run_id}: "
                    f"{runs[second_run_id]['error_type'] or 'unknown'}"
                )
            if args.deactivate_repeated_failures:
                if not args.apply:
                    print(
                        "Preview only. Add --apply to deactivate these boards."
                    )
                else:
                    print(
                        f"Deactivated {_deactivate_repeated_failures(conn, repeated)} "
                        "repeatedly failed boards."
                    )


if __name__ == "__main__":
    main()