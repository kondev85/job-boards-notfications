#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Safely validate a Workable account registry in resumable batches.

Only successful public-feed probes are added to boards.json. A 404 is recorded
as invalid; throttles, server errors, and network failures remain retryable.

Example:
    uv run scripts/validate_workable_registry.py \
        attached_assets/workable_1789592949726.csv \
        --batch-size 25 --interval 1.2
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import job_boards
import psycopg
import postgres_persistence


DEFAULT_STATE = ROOT / "reports" / "workable-validation-progress.json"
DEFAULT_PUBLISHED_AFTER = "2026-08-01"
DEFAULT_PROBE_BUDGET = 100
DEFAULT_QUOTA_COOLDOWN_SECONDS = 86_400


def _atomic_write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def _source_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_published_after(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected YYYY-MM-DD, got {value!r}"
        ) from exc


def _read_candidates(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if "slug" not in (reader.fieldnames or []):
            raise SystemExit(f"{path} must contain a slug column")
        candidates: list[str] = []
        seen: set[str] = set()
        for row in reader:
            slug = str(row.get("slug") or "").strip()
            key = slug.casefold()
            if slug and key not in seen:
                candidates.append(slug)
                seen.add(key)
    return candidates


def _load_state(
    path: Path,
    source: Path,
    candidates: list[str],
    validation_rule: str,
) -> dict:
    source_hash = _source_hash(source)
    try:
        state = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}

    stored_entries = state.get("entries") if isinstance(state, dict) else None
    if not isinstance(stored_entries, dict):
        stored_entries = {}

    reset_entries = state.get("validation_rule") != validation_rule
    old_entries = {} if reset_entries else stored_entries
    entries = {}
    for slug in candidates:
        key = slug.casefold()
        previous = old_entries.get(key)
        if isinstance(previous, dict):
            entry = dict(previous)
            entry["slug"] = slug
            entries[key] = entry
        else:
            entries[key] = {"slug": slug, "status": "pending"}

    loaded = {
        "version": 1,
        "source": str(source),
        "source_sha256": source_hash,
        "validation_rule": validation_rule,
        "entries": entries,
    }
    if state.get("next_allowed_at"):
        loaded["next_allowed_at"] = state["next_allowed_at"]
    elif state.get("updated_at"):
        retry_after = max(
            (
                float(entry.get("retry_after_seconds") or 0)
                for entry in stored_entries.values()
                if entry.get("http_status") == 429
            ),
            default=0.0,
        )
        if retry_after > 0:
            try:
                throttled_at = datetime.fromisoformat(
                    str(state["updated_at"]).replace("Z", "+00:00")
                )
            except ValueError:
                pass
            else:
                if throttled_at.tzinfo is None:
                    throttled_at = throttled_at.replace(tzinfo=timezone.utc)
                loaded["next_allowed_at"] = (
                    throttled_at + timedelta(seconds=retry_after)
                ).isoformat(timespec="seconds")
    return loaded


def _cooldown_remaining(state: dict) -> float:
    value = str(state.get("next_allowed_at") or "").strip()
    if not value:
        return 0.0
    try:
        allowed_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        state.pop("next_allowed_at", None)
        return 0.0
    if allowed_at.tzinfo is None:
        allowed_at = allowed_at.replace(tzinfo=timezone.utc)
    remaining = (allowed_at - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        state.pop("next_allowed_at", None)
        return 0.0
    return remaining


def _set_cooldown(state: dict, seconds: float) -> None:
    allowed_at = datetime.now(timezone.utc) + timedelta(seconds=max(0.0, seconds))
    state["next_allowed_at"] = allowed_at.isoformat(timespec="seconds")
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _cache_verified(cache_path: Path, slug: str) -> bool:
    boards = job_boards._read_boards(cache_path)
    known = boards.setdefault("workable", [])
    known_keys = {item.casefold() for item in known}
    if slug.casefold() in known_keys:
        return False
    known.append(slug)
    known.sort(key=str.casefold)
    job_boards._write_json_atomic(cache_path, boards)
    return True


def _cache_invalid(cache_path: Path, slug: str) -> bool:
    boards = job_boards._read_boards(cache_path)
    known = boards.setdefault("workable", [])
    retained = [item for item in known if item.casefold() != slug.casefold()]
    if len(retained) == len(known):
        return False
    boards["workable"] = retained
    job_boards._write_json_atomic(cache_path, boards)
    return True


def _summary(entries: dict[str, dict]) -> dict[str, int]:
    counts = {"verified": 0, "invalid": 0, "pending": 0, "inconclusive": 0}
    for entry in entries.values():
        status = entry.get("status", "pending")
        counts[status if status in counts else "inconclusive"] += 1
    return counts


def _sync_postgres(conn: psycopg.Connection, slugs: list[str]) -> int:
    if not slugs:
        return 0
    with conn.transaction():
        with conn.cursor() as cur:
            return postgres_persistence._sync_board_registry(
                cur,
                [("workable", slug) for slug in slugs],
                datetime.now(timezone.utc),
            )


def _deactivate_postgres(conn: psycopg.Connection, slug: str) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE job_boards SET active = FALSE "
                "WHERE ats = %s AND slug = %s AND active IS TRUE",
                ("workable", slug),
            )
            return cur.rowcount


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Workable registry slugs without losing rate-limited candidates."
    )
    parser.add_argument("registry", type=Path, help="CSV containing a slug column")
    parser.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE,
        help=f"checkpoint JSON (default: {DEFAULT_STATE.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=job_boards.BOARDS_CACHE,
        help="verified board cache to append to",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="maximum candidates to probe in this run (default: 25)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="probe all pending candidates in this resumable run",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.5,
        help="minimum seconds between probes (default: 1.5)",
    )
    parser.add_argument(
        "--success-cooldown-every",
        type=int,
        default=100,
        help="cool down after this many successful probes; 0 disables it (default: 100)",
    )
    parser.add_argument(
        "--success-cooldown",
        type=float,
        default=60.0,
        help="seconds to cool down after each success milestone (default: 60)",
    )
    parser.add_argument(
        "--probe-budget",
        type=int,
        default=DEFAULT_PROBE_BUDGET,
        help=(
            "maximum candidates to probe before a checkpointed quota cooldown; "
            f"0 disables it (default: {DEFAULT_PROBE_BUDGET})"
        ),
    )
    parser.add_argument(
        "--quota-cooldown",
        type=float,
        default=DEFAULT_QUOTA_COOLDOWN_SECONDS,
        help=(
            "seconds to wait between probe-budget windows "
            f"(default: {DEFAULT_QUOTA_COOLDOWN_SECONDS})"
        ),
    )
    parser.add_argument(
        "--max-rate-limit-wait",
        type=float,
        default=60.0,
        help="retry a 429 once when its server wait is no longer than this; "
        "longer waits stop safely (default: 60)",
    )
    parser.add_argument(
        "--missing-rate-limit-wait",
        type=float,
        default=60.0,
        help="fallback wait before one retry when a 429 has no wait header "
        "(default: 60)",
    )
    parser.add_argument(
        "--sync-postgres",
        action="store_true",
        help="also add verified slugs to the PostgreSQL job_boards registry",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="synchronize verified checkpoint entries without probing new slugs",
    )
    parser.add_argument(
        "--published-after",
        type=_parse_published_after,
        default=_parse_published_after(DEFAULT_PUBLISHED_AFTER),
        help=(
            "only verify boards with a posting on or after this date "
            f"(default: {DEFAULT_PUBLISHED_AFTER})"
        ),
    )
    args = parser.parse_args()

    if not args.registry.exists():
        parser.error(f"registry does not exist: {args.registry}")
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    if args.success_cooldown_every < 0:
        parser.error("--success-cooldown-every must be non-negative")
    if args.success_cooldown < 0:
        parser.error("--success-cooldown must be non-negative")
    if args.probe_budget < 0:
        parser.error("--probe-budget must be non-negative")
    if args.quota_cooldown < 0:
        parser.error("--quota-cooldown must be non-negative")
    if args.max_rate_limit_wait < 0:
        parser.error("--max-rate-limit-wait must be non-negative")
    if args.missing_rate_limit_wait < 0:
        parser.error("--missing-rate-limit-wait must be non-negative")
    if args.sync_only and not args.sync_postgres:
        parser.error("--sync-only requires --sync-postgres")

    candidates = _read_candidates(args.registry)
    validation_rule = f"recent-posting:{args.published_after.date().isoformat()}"
    state = _load_state(args.state, args.registry, candidates, validation_rule)
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _atomic_write(args.state, state)

    cooldown_remaining = _cooldown_remaining(state)
    if cooldown_remaining > 0:
        allowed_at = state["next_allowed_at"]
        print(
            "Workable quota cooldown is active; no requests were sent. "
            f"Resume after {allowed_at} "
            f"({cooldown_remaining / 3600:.1f} hours remaining)."
        )
        _atomic_write(args.state, state)
        return 0

    conn = None
    if args.sync_postgres:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise SystemExit(
                "DATABASE_URL is not set; omit --sync-postgres or configure the "
                "Replit-managed database"
            )
        conn = psycopg.connect(dsn)
        try:
            existing_verified = [
                entry["slug"]
                for entry in state["entries"].values()
                if entry.get("status") == "verified"
            ]
            synced = _sync_postgres(conn, existing_verified)
            if synced:
                print(f"PostgreSQL registry: synchronized {synced} verified boards")
        except Exception:
            conn.close()
            raise

    pending = [
        entry
        for entry in state["entries"].values()
        if entry.get("status") in {"pending", "inconclusive"}
    ]
    requested_batch = [] if args.sync_only else (
        pending if args.all else pending[: args.batch_size]
    )
    batch = (
        requested_batch[: args.probe_budget]
        if args.probe_budget
        else requested_batch
    )
    print(
        f"Workable registry: {len(candidates)} candidates; "
        f"{len(batch)} to probe"
        + (
            f" ({len(requested_batch)} eligible before the "
            f"{args.probe_budget}-probe budget)"
            if len(requested_batch) != len(batch)
            else ""
        )
        + f"; current {_summary(state['entries'])}"
    )
    if not batch:
        print("Nothing to validate.")
        if conn is not None:
            conn.close()
        return 0

    added = 0
    rate_limited = False
    next_request_at = 0.0
    successful_probes = 0
    for index, entry in enumerate(batch, start=1):
        wait = next_request_at - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        slug = entry["slug"]
        result = job_boards.probe_workable_board(
            slug,
            published_after=args.published_after,
        )
        next_request_at = time.monotonic() + args.interval

        if (
            result.get("http_status") == 429
        ):
            retry_wait = result.get("retry_after_seconds")
            if retry_wait is None:
                retry_wait = args.missing_rate_limit_wait
                wait_source = "fallback"
            else:
                wait_source = "server"
            if float(retry_wait) <= args.max_rate_limit_wait:
                retry_wait = max(args.interval, float(retry_wait))
                print(
                    f"{slug}: HTTP 429; waiting {retry_wait:.1f}s "
                    f"({wait_source}) before one retry"
                )
                time.sleep(retry_wait)
                result = job_boards.probe_workable_board(
                    slug,
                    published_after=args.published_after,
                )
                next_request_at = time.monotonic() + args.interval

        classification = str(result["classification"])
        entry["status"] = classification
        entry["http_status"] = result.get("http_status")
        entry["reason"] = result.get("reason")
        if result.get("retry_after_seconds") is not None:
            entry["retry_after_seconds"] = result["retry_after_seconds"]
        for key in (
            "rate_limit_limit",
            "rate_limit_remaining",
            "rate_limit_wait_seconds",
        ):
            if result.get(key) is not None:
                entry[key] = result[key]
        state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _atomic_write(args.state, state)

        if classification == "verified":
            successful_probes += 1
            if _cache_verified(args.cache, slug):
                added += 1
            if conn is not None:
                synced = _sync_postgres(conn, [slug])
                if synced:
                    print(f"  PostgreSQL registry: added {slug}")
            print(f"[{index}/{len(batch)}] {slug}: verified; added={added}")
            remaining_wait = result.get("rate_limit_wait_seconds")
            remaining = result.get("rate_limit_remaining")
            if (
                remaining == 0
                and remaining_wait is not None
                and float(remaining_wait) > 0
            ):
                next_request_at = max(
                    next_request_at,
                    time.monotonic() + float(remaining_wait),
                )
                print(
                    f"  Workable rate window exhausted; waiting "
                    f"{float(remaining_wait):.1f}s for reset"
                )
            if (
                args.success_cooldown_every
                and successful_probes % args.success_cooldown_every == 0
                and index < len(batch)
            ):
                print(
                    f"  success milestone {successful_probes}; cooling down "
                    f"{args.success_cooldown:.1f}s"
                )
                time.sleep(args.success_cooldown)
                next_request_at = time.monotonic() + args.interval
        else:
            print(
                f"[{index}/{len(batch)}] {slug}: {classification} "
                f"({result.get('reason', 'unknown')})"
            )
            if classification == "invalid":
                _cache_invalid(args.cache, slug)
                if conn is not None:
                    _deactivate_postgres(conn, slug)

        if result.get("stop"):
            rate_limited = True
            retry_after = result.get("retry_after_seconds")
            if retry_after is not None:
                _set_cooldown(state, float(retry_after))
                _atomic_write(args.state, state)
            print(
                "Stopping batch after a provider/server throttle. "
                "The candidate remains retryable in the checkpoint."
            )
            break

    print(
        f"Completed {index if batch else 0} probes; added {added}; "
        f"final {_summary(state['entries'])}"
    )
    if rate_limited:
        print(f"Resume later with the same command and checkpoint: {args.state}")
    elif (
        args.probe_budget
        and len(batch) == args.probe_budget
        and len(requested_batch) > len(batch)
    ):
        _set_cooldown(state, args.quota_cooldown)
        _atomic_write(args.state, state)
        print(
            f"Probe budget reached; checkpointed a cooldown until "
            f"{state['next_allowed_at']}. Resume with the same command after that time."
        )
    if conn is not None:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())