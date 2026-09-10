#!/usr/bin/env python3
"""Background, user-scoped matcher invoked by the authenticated API."""
from __future__ import annotations
import os, sys
from datetime import date, datetime, time, timezone
import psycopg
import postgres_persistence as persistence

def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: search_worker.py RUN_ID USER_ID")
    run_id, user_id = map(int, sys.argv[1:3])
    dsn = os.environ["DATABASE_URL"]
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE search_runs SET status='running',progress=10,updated_at=now() WHERE run_id=%s", (run_id,))
        run = conn.execute("SELECT cutoff,scope,ats,board_ids FROM search_runs WHERE run_id=%s AND owner_user_id=%s", (run_id,user_id)).fetchone()
        if not run: return
        cutoff, scope, ats, board_ids = run
        published_after = (
            datetime.combine(cutoff, time.min, tzinfo=timezone.utc)
            if isinstance(cutoff, date) and not isinstance(cutoff, datetime)
            else cutoff
        )
        selected_ats = tuple(a.strip() for a in (ats or "").split(",") if a.strip()) if scope == "ats" and ats else None
        board_filter = ""
        board_params = []
        if scope == "boards":
            board_filter = " AND b.board_id = ANY(%s)"
            board_params.append(list(board_ids or []))
        elif selected_ats:
            board_filter = " AND b.ats = ANY(%s)"
            board_params.append(list(selected_ats))
        boards = conn.execute(
            f"""SELECT b.board_id,b.ats,b.slug,c.display_name
                FROM job_boards b JOIN companies c USING(company_id)
                WHERE b.active IS TRUE AND b.closed_at IS NULL {board_filter}
                ORDER BY b.board_id""", board_params).fetchall()
        profile_json, cv_text = conn.execute(
            "SELECT profile_json, cv_text FROM users WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if not profile_json and str(cv_text or "").strip():
            persistence.generate_profile_for_user(conn, user_id=user_id)
        total = max(1, len(boards))
        failures: list[str] = []
        for index, (board_id, board_ats, slug, company) in enumerate(boards, 1):
            try:
                rows, _ = persistence._fetch_normalized(
                    board_ats, slug, published_after=published_after
                )
                now = datetime.now(timezone.utc)
                with conn.transaction():
                    persistence._upsert_board_jobs(
                        conn, board_id, company, rows, now, close_missing=False
                    )
                    conn.execute("UPDATE job_boards SET last_seen=%s,closed_at=NULL WHERE board_id=%s", (now, board_id))
            except Exception as exc:
                failures.append(f"{board_ats}/{slug}: {type(exc).__name__}")
            finally:
                conn.execute("UPDATE search_runs SET progress=%s,updated_at=now() WHERE run_id=%s", (10 + int(index / total * 55), run_id))
                conn.commit()
        selected_board_ids = tuple(int(row[0]) for row in boards) if scope == "boards" else None
        # The persistence matcher upserts only deterministic scores and never
        # changes a human state (human state lives in user_job_state).
        persistence.run_matching(conn, user_id=user_id, published_after=published_after,
                                 ats=selected_ats, board_ids=selected_board_ids)
        conn.execute("UPDATE search_runs SET progress=75,updated_at=now() WHERE run_id=%s", (run_id,))
        recommendation_failed = None
        try:
            persistence.run_recommendations(conn, user_id=user_id, published_after=published_after,
                                            ats=selected_ats, board_ids=selected_board_ids)
        except Exception as exc:
            recommendation_failed = f"recommendations failed: {type(exc).__name__}"
        notes = []
        if failures:
            notes.append(f"{len(failures)} board import(s) failed: " + ", ".join(failures[:10]))
        if recommendation_failed:
            notes.append(recommendation_failed)
        status = "completed_with_warnings" if notes else "completed"
        conn.execute("UPDATE search_runs SET status=%s,progress=100,error=%s,updated_at=now() WHERE run_id=%s", (status, " | ".join(notes) if notes else None, run_id))
        conn.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if len(sys.argv) > 1:
            try:
                with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                    conn.execute("UPDATE search_runs SET status='failed',error=%s,updated_at=now() WHERE run_id=%s", (str(exc), int(sys.argv[1])))
                    conn.commit()
            except Exception:
                pass
        raise