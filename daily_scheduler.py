#!/usr/bin/env python3
"""Single-shot scheduled search runner.

Run this from a Replit Scheduled Deployment (normally every 24 hours):
``python3 daily_scheduler.py``.  It intentionally exits after one pass.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, time, timedelta, timezone

import psycopg

import postgres_persistence as persistence


def _daily_import_args(published_after: datetime) -> argparse.Namespace:
    """Build the shared resumable importer configuration for scheduled runs."""
    return argparse.Namespace(
        ats="all",
        board=None,
        boards_from=None,
        limit=None,
        workday_bruteforce=False,
        published_after=published_after.date().isoformat(),
        greenhouse_content=False,
        match=False,
        recommend=False,
        recommend_batch_size=persistence.RECOMMENDATION_BATCH_SIZE,
        daily=True,
        daily_all_ats=True,
        resume=False,
        auto_resume=True,
        export_report=False,
        export_recommendations=False,
        generate_profile=False,
        user_email=None,
    )


def main() -> None:
    overlap = max(0, int(os.environ.get("DAILY_SEARCH_OVERLAP_DAYS", "2")))
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=overlap)
    published_after = datetime.combine(cutoff, time.min, tzinfo=timezone.utc)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))",
                            ("northstar.daily_scheduler",)).fetchone()[0]:
            return
        conn.commit()
        import_status = persistence.run(_daily_import_args(published_after))
        if import_status:
            print(
                "Scheduled board import did not complete; skipping matching and "
                "recommendations so the next scheduled invocation can resume it.",
                file=sys.stderr,
            )
            return

        users = conn.execute(
            "SELECT user_id,profile_json,cv_text FROM users WHERE active IS TRUE ORDER BY user_id"
        ).fetchall()
        for user_id, profile, cv_text in users:
            active = conn.execute(
                """SELECT run_id FROM search_runs
                   WHERE owner_user_id=%s AND status IN ('queued','running')
                   LIMIT 1""",
                (user_id,),
            ).fetchone()
            if active:
                continue
            try:
                run_id = conn.execute(
                    """INSERT INTO search_runs
                       (owner_user_id,scope,cutoff,run_type)
                       VALUES (%s,'all',%s,'scheduled') RETURNING run_id""",
                    (user_id, cutoff),
                ).fetchone()[0]
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                continue
            conn.commit()
            try:
                conn.execute(
                    "UPDATE search_runs SET status='running',progress=5,updated_at=now() WHERE run_id=%s",
                    (run_id,),
                )
                conn.commit()
                current_profile = profile
                if not current_profile and str(cv_text or "").strip():
                    persistence.generate_profile_for_user(conn, user_id=user_id)
                recommendation_failure = None
                conn.execute(
                    "UPDATE search_runs SET progress=60,updated_at=now() WHERE run_id=%s",
                    (run_id,),
                )
                conn.commit()
                persistence.run_matching(
                    conn, user_id=user_id, published_after=published_after
                )
                conn.execute(
                    "UPDATE search_runs SET progress=75,updated_at=now() WHERE run_id=%s",
                    (run_id,),
                )
                conn.commit()
                try:
                    persistence.run_recommendations(
                        conn, user_id=user_id, published_after=published_after
                    )
                except Exception as exc:
                    recommendation_failure = (
                        f"recommendations failed: {type(exc).__name__}"
                    )
                notes = []
                if recommendation_failure:
                    notes.append(recommendation_failure)
                note = " | ".join(notes) if notes else None
                status = "completed_with_warnings" if notes else "completed"
                conn.execute(
                    """UPDATE search_runs SET status=%s,progress=100,
                       error=%s,updated_at=now() WHERE run_id=%s""",
                    (status, note, run_id),
                )
                conn.commit()
            except Exception as exc:
                conn.execute(
                    "UPDATE search_runs SET status='failed',error=%s,updated_at=now() WHERE run_id=%s",
                    (str(exc), run_id),
                )
                conn.commit()


if __name__ == "__main__":
    main()