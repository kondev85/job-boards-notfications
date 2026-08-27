#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Regression tests for the PostgreSQL persistence adapter.

Run with ``uv run test_postgres_persistence.py``. Adapter tests replace the
scraper's HTTP function with fixture payloads. The database test starts a
temporary local PostgreSQL cluster, so it never touches the configured Replit
database or requires network access.
"""

from __future__ import annotations

import getpass
import json
import shutil
import socket
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import psycopg

import job_boards
import postgres_persistence as persistence


def _with_fetch(payload, fn):
    """Run fn with the scraper's network function replaced by a fixture."""
    original = job_boards.fetch
    job_boards.fetch = lambda *args, **kwargs: json.dumps(payload).encode()
    try:
        return fn()
    finally:
        job_boards.fetch = original


def _row(
    external_id: str,
    *,
    ats: str = "ashby",
    title: str = "Backend Engineer",
    description: str | None = "Build reliable systems.",
    published_at: datetime | None = None,
) -> dict:
    return {
        "ats": ats,
        "slug": "acme",
        "external_id": external_id,
        "title": title,
        "department": "Engineering",
        "team": "Platform",
        "employment_type": "Full-time",
        "location_raw": "Remote",
        "is_remote": True,
        "workplace_type": "remote",
        "published_at": published_at,
        "source_updated_at": None,
        "job_url": f"https://jobs.example.test/{external_id}",
        "description_text": description,
    }


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def _temporary_postgres() -> Iterator[str]:
    """Yield a DSN for a throwaway local PostgreSQL database."""
    required = {name: shutil.which(name) for name in ("initdb", "pg_ctl")}
    missing = [name for name, path in required.items() if path is None]
    if missing:
        raise AssertionError(
            f"PostgreSQL test tools are unavailable: {', '.join(missing)}"
        )

    with tempfile.TemporaryDirectory(prefix="postgres-persistence-test-") as tmp:
        root = Path(tmp)
        data_dir = root / "data"
        socket_dir = root / "socket"
        socket_dir.mkdir()
        port = _free_port()
        database = "persistence_regression_test"
        user = getpass.getuser()
        started = False

        subprocess.run(
            [
                required["initdb"],
                "--pgdata",
                str(data_dir),
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
                f"--username={user}",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            subprocess.run(
                [
                    required["pg_ctl"],
                    "--pgdata",
                    str(data_dir),
                    "--log",
                    str(root / "postgres.log"),
                    "--options",
                    f"-h 127.0.0.1 -k {socket_dir} -p {port}",
                    "--wait",
                    "start",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            started = True
            maintenance_dsn = (
                f"host={socket_dir} port={port} dbname=postgres user={user}"
            )
            with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
                conn.execute(f'CREATE DATABASE "{database}"')
            yield f"host={socket_dir} port={port} dbname={database} user={user}"
        finally:
            if started:
                subprocess.run(
                    [
                        required["pg_ctl"],
                        "--pgdata",
                        str(data_dir),
                        "--mode",
                        "immediate",
                        "stop",
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )


def test_adapter_mappings_are_offline_and_keep_descriptions():
    ashby_rows, skipped = _with_fetch(
        {
            "jobs": [
                {
                    "id": "ashby-1",
                    "isListed": True,
                    "title": "Backend Engineer",
                    "location": "Remote",
                    "isRemote": True,
                    "publishedAt": "2026-08-01T10:00:00Z",
                    "updatedAt": "2026-08-02T10:00:00Z",
                    "jobUrl": "https://jobs.ashby.test/ashby-1",
                    "descriptionPlain": "<p>Build &amp; ship.</p>",
                }
            ]
        },
        lambda: persistence._fetch_normalized("ashby", "acme"),
    )
    assert skipped == 0 and len(ashby_rows) == 1
    ashby = ashby_rows[0]
    assert ashby["external_id"] == "ashby-1"
    assert ashby["description_text"] == "Build & ship."
    assert ashby["published_at"] == datetime(
        2026, 8, 1, 10, tzinfo=timezone.utc
    )
    assert ashby["source_updated_at"] == datetime(
        2026, 8, 2, 10, tzinfo=timezone.utc
    )

    lever_rows, skipped = _with_fetch(
        [
            {
                "id": "lever-1",
                "text": "Platform Engineer",
                "categories": {"location": "Remote", "team": "Platform"},
                "workplaceType": "remote",
                "createdAt": 1750119882479,
                "hostedUrl": "https://jobs.lever.test/lever-1",
                "descriptionPlain": "Operate <b>critical</b> services.",
            }
        ],
        lambda: persistence._fetch_normalized("lever", "acme"),
    )
    assert skipped == 0 and len(lever_rows) == 1
    assert lever_rows[0]["title"] == "Platform Engineer"
    assert lever_rows[0]["description_text"] == "Operate critical services."
    assert lever_rows[0]["published_at"] == datetime(
        2025, 6, 17, 0, 24, 42, tzinfo=timezone.utc
    )

    greenhouse_rows, skipped = _with_fetch(
        {
            "jobs": [
                {
                    "id": 42,
                    "title": "Recruiter",
                    "location": {"name": "New York"},
                    "first_published": "2026-08-03",
                    "content": "<p>Should remain deferred.</p>",
                }
            ]
        },
        lambda: persistence._fetch_normalized("greenhouse", "acme"),
    )
    assert skipped == 0 and greenhouse_rows[0]["external_id"] == "42"
    assert greenhouse_rows[0]["description_text"] is None


def test_timestamp_conversion_handles_iso_epoch_and_invalid_values():
    assert persistence._timestamp("2026-08-01T12:30:00Z") == datetime(
        2026, 8, 1, 12, 30, tzinfo=timezone.utc
    )
    assert persistence._timestamp("2026-08-01") == datetime(
        2026, 8, 1, tzinfo=timezone.utc
    )
    assert persistence._timestamp(1750119882479) == datetime(
        2025, 6, 17, 0, 24, 42, 479000, tzinfo=timezone.utc
    )
    assert persistence._timestamp(None) is None
    assert persistence._timestamp("not a timestamp") is None


def test_postgres_schema_constraints_repeat_import_and_lifecycle():
    with _temporary_postgres() as dsn:
        with psycopg.connect(dsn) as conn:
            conn.execute(persistence.SCHEMA_SQL)
            with conn.transaction():
                with conn.cursor() as cur:
                    first_seen = datetime(2026, 8, 1, tzinfo=timezone.utc)
                    board_id = persistence._ensure_board(
                        cur, "ashby", "acme", first_seen
                    )
                    rows = [
                        _row("job-1", published_at=first_seen),
                        _row("job-2", title="Data Engineer", published_at=first_seen),
                    ]
                    inserted, closed = persistence._upsert_board_jobs(
                        cur, board_id, rows, first_seen
                    )
                    assert (inserted, closed) == (2, 0)

            tables = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                    """
                )
            }
            assert tables == {"companies", "job_boards", "jobs"}

            constraints = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = current_schema()
                      AND constraint_type = 'UNIQUE'
                    """
                )
            }
            assert any(
                "job_boards_ats_slug" in name or name.endswith("_ats_slug_key")
                for name in constraints
            )
            assert any(
                "jobs_ats_external_id" in name or name.endswith("_ats_external_id_key")
                for name in constraints
            )

            with conn.transaction():
                with conn.cursor() as cur:
                    second_seen = datetime(2026, 8, 2, tzinfo=timezone.utc)
                    board_id_again = persistence._ensure_board(
                        cur, "ashby", "acme", second_seen
                    )
                    assert board_id_again == board_id
                    existing = persistence._count_existing(
                        cur, [_row("job-1"), _row("job-2")]
                    )
                    inserted, closed = persistence._upsert_board_jobs(
                        cur,
                        board_id,
                        [
                            _row(
                                "job-1",
                                title="Backend Engineer II",
                                description=None,
                            )
                        ],
                        second_seen,
                    )
                    assert existing == 2
                    assert (inserted, closed) == (1, 1)

            counts = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM companies),
                    (SELECT COUNT(*) FROM job_boards),
                    (SELECT COUNT(*) FROM jobs)
                """
            ).fetchone()
            assert counts == (1, 1, 2)
            updated = conn.execute(
                """
                SELECT title, description_text, first_seen, last_seen, closed_at
                FROM jobs
                WHERE ats = 'ashby' AND external_id = 'job-1'
                """
            ).fetchone()
            assert updated[0] == "Backend Engineer II"
            assert updated[1] == "Build reliable systems."
            assert updated[2] == first_seen
            assert updated[3] == datetime(2026, 8, 2, tzinfo=timezone.utc)
            assert updated[4] is None

            closed_job = conn.execute(
                "SELECT closed_at FROM jobs WHERE external_id = 'job-2'"
            ).fetchone()[0]
            assert closed_job == datetime(2026, 8, 2, tzinfo=timezone.utc)

            with conn.transaction():
                with conn.cursor() as cur:
                    reopened = _row(
                        "job-2",
                        title="Data Engineer",
                        description="Reopened role.",
                    )
                    inserted, closed = persistence._upsert_board_jobs(
                        cur,
                        board_id,
                        [reopened, _row("job-1", description=None)],
                        datetime(2026, 8, 3, tzinfo=timezone.utc),
                    )
                    assert (inserted, closed) == (2, 0)
            reopened_state = conn.execute(
                "SELECT closed_at, description_text FROM jobs WHERE external_id = 'job-2'"
            ).fetchone()
            assert reopened_state == (None, "Reopened role.")


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
    print(f"ok ({len(tests)} tests)")