#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
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
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import job_boards


DEFAULT_STATE = ROOT / "reports" / "workable-validation-progress.json"


def _atomic_write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def _source_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _load_state(path: Path, source: Path, candidates: list[str]) -> dict:
    source_hash = _source_hash(source)
    try:
        state = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}

    entries = state.get("entries") if isinstance(state, dict) else None
    if not isinstance(entries, dict):
        entries = {}

    old_entries = entries
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

    return {
        "version": 1,
        "source": str(source),
        "source_sha256": source_hash,
        "entries": entries,
    }


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


def _mark_cached_entries(state: dict, cache_path: Path) -> None:
    cached = job_boards._read_boards(cache_path).get("workable", [])
    cached_keys = {slug.casefold() for slug in cached}
    for key, entry in state["entries"].items():
        if key in cached_keys:
            entry["status"] = "verified"
            entry["reason"] = "already present in boards.json"


def _summary(entries: dict[str, dict]) -> dict[str, int]:
    counts = {"verified": 0, "invalid": 0, "pending": 0, "inconclusive": 0}
    for entry in entries.values():
        status = entry.get("status", "pending")
        counts[status if status in counts else "inconclusive"] += 1
    return counts


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
        "--interval",
        type=float,
        default=1.2,
        help="minimum seconds between probes (default: 1.2)",
    )
    parser.add_argument(
        "--max-rate-limit-wait",
        type=float,
        default=30.0,
        help="retry a 429 once when its server wait is no longer than this; "
        "longer waits stop safely (default: 30)",
    )
    args = parser.parse_args()

    if not args.registry.exists():
        parser.error(f"registry does not exist: {args.registry}")
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    if args.max_rate_limit_wait < 0:
        parser.error("--max-rate-limit-wait must be non-negative")

    candidates = _read_candidates(args.registry)
    state = _load_state(args.state, args.registry, candidates)
    _mark_cached_entries(state, args.cache)
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _atomic_write(args.state, state)

    pending = [
        entry
        for entry in state["entries"].values()
        if entry.get("status") in {"pending", "inconclusive"}
    ]
    batch = pending[: args.batch_size]
    print(
        f"Workable registry: {len(candidates)} candidates; "
        f"{len(batch)} to probe; current {_summary(state['entries'])}"
    )
    if not batch:
        print("Nothing to validate.")
        return 0

    added = 0
    rate_limited = False
    previous_request = 0.0
    for index, entry in enumerate(batch, start=1):
        wait = args.interval - (time.monotonic() - previous_request)
        if previous_request and wait > 0:
            time.sleep(wait)
        slug = entry["slug"]
        previous_request = time.monotonic()
        result = job_boards.probe_workable_board(slug)

        if (
            result.get("http_status") == 429
            and result.get("retry_after_seconds") is not None
            and float(result["retry_after_seconds"]) <= args.max_rate_limit_wait
        ):
            retry_wait = max(args.interval, float(result["retry_after_seconds"]))
            print(f"{slug}: HTTP 429; waiting {retry_wait:.1f}s before one retry")
            time.sleep(retry_wait)
            previous_request = time.monotonic()
            result = job_boards.probe_workable_board(slug)

        classification = str(result["classification"])
        entry["status"] = classification
        entry["http_status"] = result.get("http_status")
        entry["reason"] = result.get("reason")
        if result.get("retry_after_seconds") is not None:
            entry["retry_after_seconds"] = result["retry_after_seconds"]
        state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _atomic_write(args.state, state)

        if classification == "verified":
            if _cache_verified(args.cache, slug):
                added += 1
            print(f"[{index}/{len(batch)}] {slug}: verified; added={added}")
        else:
            print(
                f"[{index}/{len(batch)}] {slug}: {classification} "
                f"({result.get('reason', 'unknown')})"
            )

        if result.get("stop"):
            rate_limited = True
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())