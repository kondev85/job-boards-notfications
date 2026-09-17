#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Pull public job postings from Ashby, Greenhouse, Lever, SmartRecruiters, Workday,
Recruitee, Teamtailor, and Workable boards.

No API key. Each ATS publishes an unauthenticated per-company posting API with no
global search, so this runs in two phases: discover board slugs from the Internet
Archive, then fetch and filter every board.

    uv run job_boards.py --all                              # every job, all platforms
    uv run job_boards.py --title "software engineer"
    uv run job_boards.py --title "software engineer" --match exact
    uv run job_boards.py --ats greenhouse --title "swe"     # one platform
    uv run job_boards.py --ats ashby,lever --all            # a subset
    uv run job_boards.py --grep '\\brust\\b|\\bgolang\\b'     # search descriptions
    uv run job_boards.py --refresh-boards

Results go to CSV and JSON, and accumulate into a SQLite database keyed on
(ats, posting id) so first_seen/last_seen/closed_at survive across scrapes.

boards.seed.json ships with the repo, so --refresh-boards is optional. See the README.
"""

# Keeps `X | None` annotations from being evaluated at import, so the file also
# imports under Python 3.9 — which is what a bare `python3` is on macOS, and what
# tooling gets when uv is not on its PATH. uv still provisions 3.11 per the
# metadata above; this only makes the no-uv fallback in AGENTS.md work.
from __future__ import annotations

import argparse
import csv
from email.utils import parsedate_to_datetime
import gzip
import http.client
import json
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from html import unescape
from pathlib import Path

HERE = Path(__file__).parent
# Two files on purpose. The seed is small, curated and committed, so a fresh clone
# works without touching any archive. The cache is whatever the last crawl produced
# — potentially several vendors' entire customer lists — and is gitignored, so a full
# crawl never turns this repo into published competitive intelligence.
BOARDS_SEED = HERE / "boards.seed.json"
BOARDS_CACHE = HERE / "boards.json"
WORKDAY_DISCOVERY_REPORT = HERE / "workday-discovery-report.json"
WORKDAY_DISCOVERY_PROGRESS = HERE / "workday-discovery-progress.json"
COLLINFO = "https://index.commoncrawl.org/collinfo.json"
WAYBACK_CDX = (
    "https://web.archive.org/cdx/search/cdx?url={domain}"
    "&matchType=domain&fl=original&collapse=urlkey&output=json"
)
URLSCAN_SEARCH = "https://urlscan.io/api/v1/search/?q=page.domain%3A{domain}&size=10000"
_SLUG_SHAPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{0,60}$")
_SLUG_JUNK = {
    "_next", "api", "static", "assets", "meeting", "b", "favicon.ico",
    "robots.txt", "sitemap.xml", "embed", "css", "js", "images", "img",
}

# Archive operators ask that clients identify themselves. Set JOB_SCRAPER_CONTACT to
# your own email so a server operator can reach *you* about *your* traffic.
#
# Must be ASCII: http.client encodes headers as latin-1, so a stray em-dash or an
# accented character here makes every single request raise before it leaves the
# process. Non-ASCII is stripped rather than allowed to break the run.
_CONTACT = (
    os.environ.get("JOB_SCRAPER_CONTACT")
    or os.environ.get("ASHBY_SCRAPER_CONTACT")  # the name this had before Greenhouse
    or "set JOB_SCRAPER_CONTACT"
)
UA = f"job-boards-scraper/1.0 (public posting APIs; contact: {_CONTACT})".encode(
    "ascii", "ignore"
).decode()

# `ats` and `company` identify the board; the rest is normalised from whichever API
# it came from. `matched` holds --grep context and is empty without it.
FIELDS = [
    "ats", "company", "id", "title", "department", "team", "employmentType",
    "location", "isRemote", "workplaceType", "address", "publishedAt", "jobUrl",
    "matched",
]

_HTML_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"\s+")
_SMARTRECRUITERS_DETAIL_CONCURRENCY = 8


class NotFound(Exception):
    """Board slug returned 404 — not a customer of that ATS (or never was)."""


class NotModified(Exception):
    """Server answered 304: the body is byte-identical to what we last fetched."""


class RateLimited(Exception):
    """Common Crawl returned 503. Per their docs this means the request rate was
    too high; a repeatedly-abusive IP can be blocked for 24 hours."""


# Hosts whose connections are pooled. Only the posting APIs: they take one request
# per board — over 13,000 in a full run — and a fresh TLS handshake for each was
# measured at 102ms against 64ms on a reused connection. Everything else (the
# archives, urlscan) is a handful of requests per run and may redirect, which
# urlopen handles and a raw connection would not, so those stay on urlopen.
_POOLED_HOSTS = {
    "api.ashbyhq.com",
    "boards-api.greenhouse.io",
    "api.lever.co",
    "api.smartrecruiters.com",
    "apply.workable.com",
}
# One connection per thread per host. Sharing across threads would need a lock and
# serialise the pool; a thread-local dict keeps the 8 workers independent, so a full
# run opens ~8 connections per host rather than one per board.
_CONNECTIONS = threading.local()


def _lower_headers(items) -> dict:
    """Lowercase header names.

    urlopen returns an email.message.Message, which looks keys up case-insensitively.
    A plain dict does not, so the pooled path has to normalise or a vendor changing
    `Content-Encoding` to `content-encoding` would silently skip gunzipping and hand
    back compressed bytes. Not hypothetical: the posting APIs already disagree about
    the casing of `ETag`.
    """
    return {k.lower(): v for k, v in (items.items() if hasattr(items, "items") else items)}


def _pooled_request(
    url: str,
    method: str,
    timeout: int,
    headers: dict,
    body: bytes | None = None,
) -> tuple[int, dict, bytes]:
    """One request over a reused per-thread connection. Returns (status, headers, body).

    A pooled connection can be closed by the server between requests, which surfaces
    as an exception on the next use rather than at close time, so a dead connection is
    dropped and retried once before giving up.
    """
    parts = urllib.parse.urlsplit(url)
    pool = getattr(_CONNECTIONS, "pool", None)
    if pool is None:
        pool = _CONNECTIONS.pool = {}
    target = parts.path + (f"?{parts.query}" if parts.query else "")

    for attempt in (0, 1):
        conn = pool.get(parts.netloc)
        if conn is None:
            conn = pool[parts.netloc] = http.client.HTTPSConnection(
                parts.netloc, timeout=timeout
            )
        try:
            if body is None:
                conn.request(method, target, headers=headers)
            else:
                conn.request(method, target, body=body, headers=headers)
            resp = conn.getresponse()
            body = resp.read()  # must drain, or the connection cannot be reused
            return resp.status, _lower_headers(resp.getheaders()), body
        except (http.client.HTTPException, OSError):
            try:
                conn.close()
            except Exception:
                pass
            pool.pop(parts.netloc, None)
            if attempt:
                raise
    raise RuntimeError("unreachable")


def _single_request(
    url: str, method: str, timeout: int, etag: str | None = None
) -> tuple[int, dict, bytes]:
    """One request, pooled where that is safe and via urlopen everywhere else."""
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip"}
    if etag:
        headers["If-None-Match"] = etag
    if _is_pooled_host(urllib.parse.urlsplit(url).netloc):
        return _pooled_request(url, method, timeout, headers)
    req = urllib.request.Request(url, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, _lower_headers(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, _lower_headers(e.headers or {}), e.read()


def _is_pooled_host(netloc: str) -> bool:
    """Use connection pooling for posting APIs and Workday career hosts."""
    hostname = netloc.split(":", 1)[0].lower()
    return (
        netloc in _POOLED_HOSTS
        or hostname.endswith(".myworkdayjobs.com")
        or hostname.endswith(".recruitee.com")
        or hostname.endswith(".teamtailor.com")
    )


# ponytail: seconds-form Retry-After only. The HTTP-date form is legal but none of
# these APIs send it, and falling back to the exponential delay is already correct.
def _retry_delay(retry_after: str | None, attempt: int, cap: float = 30.0) -> float:
    """How long to wait before retrying a throttled request.

    Capped: a server asking for an hour would stall a 13,000-board run behind one
    slug, and at that point giving up and logging the board is the better trade.
    """
    try:
        return min(float(retry_after), cap)
    except (TypeError, ValueError):
        return float(2**attempt)


def fetch(
    url: str,
    timeout: int = 30,
    retries: int = 4,
    method: str = "GET",
    etag: str | None = None,
    meta: dict | None = None,
) -> bytes:
    """GET a URL, transparently gunzipping. Raises NotFound on 404.

    Common Crawl's CDX index 502/504s under load often enough that a single
    attempt fails maybe half the time, so 5xx gets exponential backoff.

    429 and 403 get the same backoff. They are 4xx, so without this they took the
    raise-immediately path and a throttled board was dropped for the whole run: a
    real Greenhouse scrape lost 8 consecutive slugs that way. `Retry-After` wins
    over the exponential delay when the server sends it, since that is the server
    telling us exactly how long it wants.
    """
    for attempt in range(retries):
        try:
            status, headers, body = _single_request(url, method, timeout, etag)
            if meta is not None:
                meta["etag"] = headers.get("etag")
            if status == 304:
                raise NotModified(url)
            if status == 404:
                raise NotFound(url)
            if status >= 500:
                if attempt == retries - 1:
                    if status == 503:
                        raise RateLimited(url)
                    raise urllib.error.HTTPError(url, status, "server error", None, None)
                time.sleep(_retry_delay(headers.get("retry-after"), attempt))
                continue
            if status in (429, 403):
                if attempt == retries - 1:
                    raise urllib.error.HTTPError(url, status, "throttled", None, None)
                time.sleep(_retry_delay(headers.get("retry-after"), attempt))
                continue
            if status >= 400:
                raise urllib.error.HTTPError(url, status, "client error", None, None)
            if headers.get("content-encoding") == "gzip":
                body = gzip.decompress(body)
            return body
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError):
            if attempt == retries - 1:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("unreachable")


def plain_text(value: str) -> str:
    """Strip HTML tags, decode entities, collapse whitespace."""
    return _SPACE.sub(" ", unescape(_HTML_TAG.sub(" ", value))).strip()


# --------------------------------------------------------------------------- #
# Per-ATS adapters
#
# Each API returns a different shape, so a normaliser maps it onto FIELDS and
# everything downstream — filters, CSV, SQLite — stays platform-agnostic. A
# normaliser returns None for a posting that should not be listed at all.
#
# `_description` is stripped off before the row is emitted; only --grep reads it.
# --------------------------------------------------------------------------- #


def _provider_datetime(value: object) -> str:
    """Normalize provider ISO and trailing-UTC timestamps to ISO UTC."""
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(" UTC"):
        text = text[:-4] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _location_label(value: object) -> str:
    if isinstance(value, dict):
        return str(
            value.get("name")
            or ", ".join(
                str(value.get(key))
                for key in ("city", "state", "region", "country")
                if value.get(key)
            )
            or ", ".join(
                str(value.get(key))
                for key in ("addressLocality", "addressRegion", "addressCountry")
                if value.get(key)
            )
            or ""
        ).strip()
    return str(value or "").strip()


def _location_collection(value: object) -> list[dict]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def normalize_recruitee(job: dict) -> dict | None:
    """Normalize a public Recruitee Careers Site offer."""
    posting_id = str(job.get("id") or job.get("guid") or "").strip()
    title = str(job.get("title") or "").strip()
    status = str(job.get("status") or "published").strip().lower()
    if not posting_id or not title or status != "published":
        return None

    locations = _location_collection(job.get("locations"))
    labels = [_location_label(location) for location in locations]
    labels = list(dict.fromkeys(label for label in labels if label))
    if not labels:
        fallback = _location_label(job.get("location"))
        if fallback:
            labels = [fallback]

    workplace = ""
    if job.get("remote"):
        workplace = "remote"
    elif job.get("hybrid"):
        workplace = "hybrid"
    elif job.get("on_site"):
        workplace = "onsite"

    translations = job.get("translations")
    translated = translations.get("en") if isinstance(translations, dict) else None
    translated = translated if isinstance(translated, dict) else {}
    description_parts = [
        translated.get("description") or job.get("description") or "",
        translated.get("requirements") or job.get("requirements") or "",
        translated.get("highlight") or job.get("highlight") or "",
    ]
    return {
        "id": posting_id,
        "title": title,
        "department": job.get("department") or "",
        "team": job.get("team") or "",
        "employmentType": (
            job.get("employment_type")
            or job.get("employment_type_code")
            or ""
        ),
        "location": "; ".join(labels),
        "isRemote": workplace == "remote",
        "workplaceType": workplace,
        "address": {
            key: value
            for key, value in (
                ("locations", locations or None),
                ("country", job.get("country")),
                ("country_code", job.get("country_code")),
            )
            if value is not None
        } or None,
        "publishedAt": _provider_datetime(job.get("published_at")),
        "jobUrl": job.get("careers_url") or job.get("careers_apply_url") or "",
        "_description": "\n\n".join(
            str(part) for part in description_parts if part
        ),
        "_sourceUpdatedAt": job.get("updated_at") or "",
    }


def recruitee_board_url(slug: str) -> str:
    """Return the public Recruitee offers endpoint.

    The supplied registry contains mostly ``company.recruitee.com`` boards, but
    it also contains a small number of custom domains. A dotted identifier is
    therefore treated as an already-qualified host.
    """
    host = str(slug).strip()
    if "." not in host:
        host = f"{host}.recruitee.com"
    return f"https://{host}/api/offers/"


def fetch_recruitee_jobs(
    slug: str,
    published_after: datetime | None = None,
) -> list[dict]:
    payload = json.loads(fetch(recruitee_board_url(slug), timeout=30, retries=3))
    jobs = payload.get("offers") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise ValueError(f"recruitee/{slug}: response has no offers array")
    return jobs


def recruitee_board_exists(slug: str) -> bool:
    try:
        payload = json.loads(
            fetch(recruitee_board_url(slug), timeout=25, retries=2)
        )
        return isinstance(payload, dict) and isinstance(payload.get("offers"), list)
    except Exception:
        return False


def normalize_teamtailor(job: dict) -> dict | None:
    """Normalize a Teamtailor JSON Feed item and its embedded JobPosting data."""
    posting_id = str(job.get("id") or "").strip()
    title = str(job.get("title") or "").strip()
    if not posting_id or not title:
        return None

    jobposting = job.get("_jobposting")
    jobposting = jobposting if isinstance(jobposting, dict) else {}
    locations = _location_collection(jobposting.get("jobLocation"))
    labels = []
    for location in locations:
        address = location.get("address") if isinstance(location, dict) else None
        label = _location_label(address if isinstance(address, dict) else location)
        if label and label not in labels:
            labels.append(label)

    location_type = str(jobposting.get("jobLocationType") or "").strip()
    workplace_key = location_type.casefold().replace("-", "").replace("_", "")
    if workplace_key in {"telecommute", "remote"}:
        workplace = "remote"
    elif locations:
        workplace = "onsite"
    else:
        workplace = ""

    department = jobposting.get("occupationalCategory") or job.get("department") or ""
    employment = jobposting.get("employmentType") or job.get("employmentType") or ""
    return {
        "id": posting_id,
        "title": title,
        "department": department,
        "team": job.get("team") or "",
        "employmentType": employment,
        "location": "; ".join(labels),
        "isRemote": workplace == "remote",
        "workplaceType": workplace,
        "address": {"jobLocation": locations} if locations else None,
        "publishedAt": _provider_datetime(
            job.get("date_published") or jobposting.get("datePosted")
        ),
        "jobUrl": job.get("url") or "",
        "_description": job.get("content_html") or job.get("description") or "",
        "_sourceUpdatedAt": job.get("date_modified") or jobposting.get("dateModified") or "",
    }


def teamtailor_board_url(slug: str) -> str:
    return (
        f"https://{urllib.parse.quote(str(slug).strip(), safe='')}.teamtailor.com"
        "/jobs.json?per_page=100"
    )


def fetch_teamtailor_jobs(
    slug: str,
    published_after: datetime | None = None,
) -> list[dict]:
    """Fetch every page of a Teamtailor JSON Feed."""
    next_url = teamtailor_board_url(slug)
    visited: set[str] = set()
    jobs: list[dict] = []
    for _ in range(1000):
        if not next_url or next_url in visited:
            break
        visited.add(next_url)
        payload = json.loads(fetch(next_url, timeout=30, retries=3))
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError(f"teamtailor/{slug}: response has no items array")
        jobs.extend(item for item in payload["items"] if isinstance(item, dict))
        next_url = payload.get("next_url") or ""
        if next_url and not str(next_url).startswith("http"):
            next_url = urllib.parse.urljoin(teamtailor_board_url(slug), str(next_url))
    else:
        raise ValueError(f"teamtailor/{slug}: pagination exceeded 1000 pages")
    return jobs


def teamtailor_board_exists(slug: str) -> bool:
    try:
        payload = json.loads(
            fetch(teamtailor_board_url(slug), timeout=25, retries=2)
        )
        return isinstance(payload, dict) and isinstance(payload.get("items"), list)
    except Exception:
        return False


def workable_board_url(slug: str) -> str:
    account = urllib.parse.quote(str(slug).strip(), safe="")
    return f"https://apply.workable.com/api/v3/accounts/{account}/jobs"


def workable_career_url(slug: str) -> str:
    return f"https://apply.workable.com/{urllib.parse.quote(str(slug).strip(), safe='')}/"


def workable_job_url(slug: str, shortcode: str) -> str:
    return (
        f"{workable_career_url(slug)}j/"
        f"{urllib.parse.quote(str(shortcode).strip(), safe='')}/"
    )


def _workable_post_json(url: str, payload: dict, timeout: int = 30) -> dict:
    """POST to Workable's public career-site list endpoint with retries."""
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    for attempt in range(4):
        try:
            if _is_pooled_host(urllib.parse.urlsplit(url).netloc):
                status, response_headers, response_body = _pooled_request(
                    url, "POST", timeout, headers, body
                )
                if status == 404:
                    raise NotFound(url)
                if status >= 400:
                    if status not in (429, 500, 502, 503, 504) or attempt == 3:
                        raise urllib.error.HTTPError(
                            url, status, "Workable request failed",
                            response_headers, None,
                        )
                    time.sleep(_retry_delay(response_headers.get("retry-after"), attempt))
                    continue
                result = json.loads(response_body)
            else:
                request = urllib.request.Request(
                    url, data=body, headers=headers, method="POST"
                )
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    result = json.loads(response.read())
            if not isinstance(result, dict):
                raise ValueError("Workable response is not an object")
            return result
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise NotFound(url) from exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise
            time.sleep(_retry_delay(exc.headers.get("Retry-After"), attempt))
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError):
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("unreachable")


def _workable_rate_limit_wait(headers: dict[str, str]) -> float | None:
    """Return the longest server-provided wait from Workable response headers."""
    waits: list[float] = []
    retry_after = headers.get("retry-after")
    if retry_after:
        try:
            waits.append(max(0.0, float(retry_after)))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after).timestamp()
            except (TypeError, ValueError, OverflowError):
                retry_at = 0.0
            if retry_at:
                waits.append(max(0.0, retry_at - time.time()))

    reset = headers.get("x-rate-limit-reset")
    if reset:
        try:
            waits.append(max(0.0, float(reset) - time.time()))
        except ValueError:
            pass
    return max(waits) if waits else None


def _workable_rate_limit_metadata(headers: dict[str, str]) -> dict[str, object]:
    """Extract Workable's documented rate-limit headers for the caller."""
    metadata: dict[str, object] = {}
    observed_headers = {
        name: headers[name]
        for name in (
            "retry-after",
            "x-rate-limit-limit",
            "x-rate-limit-remaining",
            "x-rate-limit-reset",
        )
        if headers.get(name) is not None
    }
    if observed_headers:
        metadata["rate_limit_headers"] = observed_headers
    for header, key in (
        ("x-rate-limit-limit", "rate_limit_limit"),
        ("x-rate-limit-remaining", "rate_limit_remaining"),
    ):
        value = headers.get(header)
        if value is None:
            continue
        try:
            metadata[key] = int(value)
        except (TypeError, ValueError):
            pass

    reset = headers.get("x-rate-limit-reset")
    if reset:
        try:
            metadata["rate_limit_reset_at"] = float(reset)
        except (TypeError, ValueError):
            pass
    wait = _workable_rate_limit_wait(headers)
    if wait is not None:
        metadata["rate_limit_wait_seconds"] = wait
    return metadata


def probe_workable_board(
    slug: str,
    timeout: int = 25,
    published_after: datetime | None = None,
) -> dict[str, object]:
    """Make one Workable registry probe without hiding its outcome.

    The normal posting fetcher retries because it is appropriate for a single
    board import. Registry validation needs the opposite behavior: preserve
    404 versus 429/5xx and let the caller checkpoint before deciding whether
    to continue. When ``published_after`` is supplied, verification also
    requires at least one posting published on or after that cutoff.
    """
    url = workable_board_url(slug)
    body = b"{}"
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        status, response_headers, response_body = _pooled_request(
            url, "POST", timeout, headers, body
        )
    except Exception as exc:
        return {
            "classification": "inconclusive",
            "http_status": None,
            "reason": f"{type(exc).__name__}: {exc}",
            "stop": False,
        }

    rate_limit_metadata = _workable_rate_limit_metadata(response_headers)

    if status == 404:
        return {
            "classification": "invalid",
            "http_status": status,
            "reason": "HTTP 404",
            "stop": False,
            **rate_limit_metadata,
        }

    if status == 200:
        try:
            result = json.loads(response_body)
        except (TypeError, json.JSONDecodeError) as exc:
            return {
                "classification": "inconclusive",
                "http_status": status,
                "reason": f"invalid JSON response: {exc}",
                "stop": False,
                **rate_limit_metadata,
            }
        if isinstance(result, dict) and isinstance(result.get("results"), list):
            jobs = result["results"]
            if published_after is not None:
                recent_jobs = [
                    item
                    for item in jobs
                    if isinstance(item, dict)
                    and published_within(
                        _provider_datetime(item.get("published")),
                        published_after,
                    )
                ]
                if not recent_jobs:
                    return {
                        "classification": "invalid",
                        "http_status": status,
                        "reason": (
                            "no jobs published on or after "
                            f"{published_after.date().isoformat()}"
                        ),
                        "stop": False,
                        **rate_limit_metadata,
                    }
                reason = (
                    f"{len(recent_jobs)} job(s) published on or after "
                    f"{published_after.date().isoformat()}"
                )
            else:
                reason = f"results array ({len(jobs)} jobs)"
            return {
                "classification": "verified",
                "http_status": status,
                "reason": reason,
                "stop": False,
                **rate_limit_metadata,
            }
        return {
            "classification": "inconclusive",
            "http_status": status,
            "reason": "200 response has no results array",
            "stop": False,
            **rate_limit_metadata,
        }

    wait = rate_limit_metadata.get("rate_limit_wait_seconds")
    return {
        "classification": "inconclusive",
        "http_status": status,
        "reason": f"HTTP {status}",
        "retry_after_seconds": wait,
        "stop": status == 429 or status >= 500,
        **rate_limit_metadata,
    }


def _workable_detail(slug: str, shortcode: str) -> dict:
    url = (
        f"https://apply.workable.com/api/v2/accounts/"
        f"{urllib.parse.quote(str(slug).strip(), safe='')}/jobs/"
        f"{urllib.parse.quote(str(shortcode).strip(), safe='')}"
    )
    return json.loads(fetch(url, timeout=30, retries=3))


def normalize_workable(job: dict) -> dict | None:
    """Normalize a Workable public career-site job."""
    posting_id = str(job.get("shortcode") or job.get("id") or "").strip()
    title = str(job.get("title") or "").strip()
    if not posting_id or not title:
        return None
    if job.get("state") and str(job.get("state")).lower() != "published":
        return None

    locations = _location_collection(job.get("locations"))
    if not locations and isinstance(job.get("location"), dict):
        locations = [job["location"]]
    labels = [_location_label(location) for location in locations]
    labels = list(dict.fromkeys(label for label in labels if label))
    workplace_key = str(job.get("workplace") or "").casefold().replace("-", "_")
    if job.get("remote") or workplace_key == "remote":
        workplace = "remote"
    elif workplace_key in {"hybrid", "on_site", "onsite"}:
        workplace = "hybrid" if workplace_key == "hybrid" else "onsite"
    else:
        workplace = ""
    department = job.get("department") or ""
    if isinstance(department, list):
        department = ", ".join(str(item) for item in department if item)
    return {
        "id": posting_id,
        "title": title,
        "department": department,
        "team": job.get("function") or "",
        "employmentType": job.get("employment_type") or "",
        "location": "; ".join(labels),
        "isRemote": workplace == "remote",
        "workplaceType": workplace,
        "address": {"locations": locations} if locations else None,
        "publishedAt": _provider_datetime(job.get("published")),
        "jobUrl": job.get("url") or "",
        "_description": "\n\n".join(
            str(job.get(key) or "")
            for key in ("description", "requirements", "benefits")
            if job.get(key)
        ),
        "_sourceUpdatedAt": job.get("updated_at") or job.get("updatedAt") or "",
    }


_WORKABLE_DETAIL_CONCURRENCY = 3


def fetch_workable_jobs(
    slug: str,
    published_after: datetime | None = None,
    detail_concurrency: int | None = None,
) -> list[dict]:
    """Fetch Workable's complete public list and enrich cutoff-eligible jobs."""
    payload = _workable_post_json(workable_board_url(slug), {})
    jobs = payload.get("results")
    if not isinstance(jobs, list):
        raise ValueError(f"workable/{slug}: response has no results array")
    total = payload.get("total")
    if total is not None and int(total) != len(jobs):
        raise ValueError(
            f"workable/{slug}: response total {total} differs from results {len(jobs)}"
        )

    eligible: list[dict] = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        if published_after is not None:
            published = _provider_datetime(item.get("published"))
            if not published:
                continue
            if datetime.fromisoformat(published) < published_after:
                continue
        eligible.append(item)

    def enrich(item: dict) -> dict:
        shortcode = str(item.get("shortcode") or "").strip()
        if not shortcode:
            return item
        try:
            detail = _workable_detail(slug, shortcode)
        except Exception as exc:
            print(
                f"  workable/{slug}/{shortcode}: detail request failed "
                f"({type(exc).__name__}: {exc})",
                file=sys.stderr,
            )
            return item
        merged = dict(item)
        merged.update(detail)
        return merged

    with ThreadPoolExecutor(
        max_workers=detail_concurrency or _WORKABLE_DETAIL_CONCURRENCY
    ) as pool:
        return list(pool.map(enrich, eligible))


def _workable_board_exists(slug: str) -> bool:
    try:
        payload = _workable_post_json(workable_board_url(slug), {}, timeout=25)
        return isinstance(payload, dict) and isinstance(payload.get("results"), list)
    except Exception:
        return False


def normalize_ashby(job: dict) -> dict | None:
    if not job.get("isListed"):
        return None
    address = {}
    if job.get("address") is not None:
        address["primary"] = job["address"]
    if job.get("secondaryLocations") is not None:
        address["secondaryLocations"] = job["secondaryLocations"]
    return {
        "id": str(job.get("id", "")),
        "title": job.get("title") or "",
        "department": job.get("department") or "",
        "team": job.get("team") or "",
        "employmentType": job.get("employmentType") or "",
        "location": job.get("location") or "",
        "isRemote": bool(job.get("isRemote")),
        "workplaceType": job.get("workplaceType") or "",
        "address": address or None,
        "publishedAt": job.get("publishedAt") or "",
        "jobUrl": job.get("jobUrl") or "",
        "_description": job.get("descriptionPlain") or job.get("descriptionHtml") or "",
    }


def normalize_greenhouse(job: dict) -> dict | None:
    # location is a nested object, not a string. Greenhouse exposes no remote flag
    # and no department on this endpoint, so remoteness is inferred from the label.
    loc = job.get("location") or {}
    # `or ""` rather than a get() default: Greenhouse sends {"name": null}, where
    # the key exists so the default never applies and the value stays None.
    name = (loc.get("name") or "") if isinstance(loc, dict) else str(loc)
    address = {}
    if job.get("location") is not None:
        address["location"] = job["location"]
    if job.get("offices") is not None:
        address["offices"] = job["offices"]
    if job.get("metadata") is not None:
        address["metadata"] = job["metadata"]
    return {
        "id": str(job.get("id", "")),
        "title": job.get("title") or "",
        "department": "",
        "team": "",
        "employmentType": "",
        "location": name,
        "isRemote": "remote" in name.lower(),
        "workplaceType": "",
        "address": address or None,
        "publishedAt": job.get("first_published") or job.get("updated_at") or "",
        "jobUrl": job.get("absolute_url") or "",
        # Absent unless the request asked for ?content=true — see SOURCES.
        "_description": job.get("content") or "",
    }


def normalize_lever(job: dict) -> dict | None:
    # Two traps here. The title field is `text`, not `title` — reading `title` gives
    # a silently empty column. And createdAt is epoch milliseconds, which has to
    # become ISO or it sorts and compares wrongly against the other platforms.
    cat = job.get("categories") or {}
    created = job.get("createdAt")
    published = ""
    if isinstance(created, (int, float)):
        published = datetime.fromtimestamp(
            created / 1000, timezone.utc
        ).isoformat(timespec="seconds")
    workplace = job.get("workplaceType") or ""
    address = {}
    if job.get("country") is not None:
        address["country"] = job["country"]
    if cat.get("location") is not None:
        address["location"] = cat["location"]
    if cat.get("allLocations") is not None:
        address["allLocations"] = cat["allLocations"]
    return {
        "id": str(job.get("id", "")),
        "title": job.get("text") or "",
        "department": cat.get("department") or "",
        "team": cat.get("team") or "",
        "employmentType": cat.get("commitment") or "",
        "location": cat.get("location") or "",
        "isRemote": workplace.lower() == "remote",
        "workplaceType": workplace,
        "address": address or None,
        "publishedAt": published,
        "jobUrl": job.get("hostedUrl") or "",
        "_description": " ".join(
            filter(None, (job.get("descriptionPlain"), job.get("descriptionBodyPlain")))
        ),
    }


def _smartrecruiters_location(location: object) -> tuple[str, dict | None]:
    if not isinstance(location, dict):
        return "", None
    label = location.get("fullLocation") or ", ".join(
        str(value)
        for value in (
            location.get("city"),
            location.get("region"),
            location.get("country"),
        )
        if value
    )
    return str(label or ""), location


def _smartrecruiters_sections_description(job: dict) -> str:
    job_ad = job.get("jobAd") or {}
    sections = job_ad.get("sections") if isinstance(job_ad, dict) else {}
    if not isinstance(sections, dict):
        return ""
    # CompanyDescription is repeated boilerplate and is not role evidence.
    preferred = ("jobDescription", "qualifications", "additionalInformation")
    return "\n\n".join(
        str(sections[name].get("text") or "")
        for name in preferred
        if isinstance(sections.get(name), dict) and sections[name].get("text")
    )


def normalize_smartrecruiters(job: dict) -> dict | None:
    posting_id = str(job.get("id") or "").strip()
    title = str(job.get("name") or "").strip()
    if not posting_id or not title:
        return None
    location, address = _smartrecruiters_location(job.get("location"))
    department = job.get("department") or {}
    function = job.get("function") or {}
    employment = job.get("typeOfEmployment") or {}
    location_data = job.get("location") or {}
    if isinstance(location_data, dict):
        if location_data.get("remote"):
            workplace = "remote"
        elif location_data.get("hybrid"):
            workplace = "hybrid"
        else:
            workplace = "onsite"
    else:
        workplace = ""
    return {
        "id": posting_id,
        "title": title,
        "department": (
            department.get("label", "")
            if isinstance(department, dict)
            else str(department)
        ),
        "team": (
            function.get("label", "")
            if isinstance(function, dict)
            else str(function)
        ),
        "employmentType": (
            employment.get("label", "")
            if isinstance(employment, dict)
            else str(employment)
        ),
        "location": location,
        "isRemote": workplace == "remote",
        "workplaceType": workplace,
        "address": address,
        "publishedAt": job.get("releasedDate") or "",
        "jobUrl": job.get("postingUrl") or job.get("applyUrl") or "",
        "_description": _smartrecruiters_sections_description(job),
        "_sourceUpdatedAt": job.get("updatedDate") or job.get("updatedAt") or "",
    }


def smartrecruiters_board_url(identifier: str) -> str:
    return (
        "https://api.smartrecruiters.com/v1/companies/"
        f"{urllib.parse.quote(str(identifier), safe='')}/postings"
    )


def _smartrecruiters_detail_url(identifier: str, posting_id: str) -> str:
    return (
        f"{smartrecruiters_board_url(identifier)}/"
        f"{urllib.parse.quote(str(posting_id), safe='')}"
    )


def _smartrecruiters_detail(identifier: str, posting_id: str) -> dict:
    return json.loads(
        fetch(_smartrecruiters_detail_url(identifier, posting_id), timeout=30, retries=3)
    )


def _smartrecruiters_career_page_exists(identifier: str) -> bool:
    url = (
        "https://careers.smartrecruiters.com/"
        f"{urllib.parse.quote(str(identifier), safe='')}"
    )
    request = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            final_path = urllib.parse.urlparse(response.geturl()).path.strip("/")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
        return False
    first_segment = urllib.parse.unquote(final_path).split("/", 1)[0]
    return first_segment.casefold() == str(identifier).strip().casefold()


def fetch_smartrecruiters_jobs(
    identifier: str,
    published_after: datetime | None = None,
    detail_concurrency: int | None = None,
) -> list[dict]:
    """Fetch active public postings and enrich them with job details."""
    limit = 100
    offset = 0
    postings: list[dict] = []
    while True:
        params = {"limit": str(limit), "offset": str(offset)}
        if published_after is not None:
            params["releasedAfter"] = published_after.isoformat(
                timespec="milliseconds"
            )
        url = smartrecruiters_board_url(identifier) + "?" + urllib.parse.urlencode(params)
        payload = json.loads(fetch(url, timeout=30, retries=3))
        page = payload.get("content")
        if not isinstance(page, list):
            raise ValueError(
                f"smartrecruiters/{identifier}: response has no content array"
            )
        postings.extend(item for item in page if isinstance(item, dict))
        total = int(payload.get("totalFound") or 0)
        if len(page) < limit or (total and offset + len(page) >= total):
            break
        offset += len(page)

    def enrich(item: dict) -> dict:
        posting_id = str(item.get("id") or "")
        if not posting_id:
            return item
        try:
            detail = _smartrecruiters_detail(identifier, posting_id)
        except Exception as exc:
            # Keep the listing data usable; a later import will retry details.
            print(
                f"  smartrecruiters/{identifier}/{posting_id}: detail request failed "
                f"({exc})",
                file=sys.stderr,
            )
            return item
        merged = dict(item)
        merged.update(detail)
        return merged

    with ThreadPoolExecutor(
        max_workers=detail_concurrency or _SMARTRECRUITERS_DETAIL_CONCURRENCY
    ) as pool:
        return list(pool.map(enrich, postings))


def smartrecruiters_board_exists(identifier: str) -> bool:
    try:
        url = smartrecruiters_board_url(identifier) + "?limit=1&offset=0"
        payload = json.loads(fetch(url, timeout=25, retries=2))
        if not isinstance(payload.get("content"), list):
            return False
        if int(payload.get("totalFound") or 0) > 0:
            return True
        return _smartrecruiters_career_page_exists(identifier)
    except Exception:
        return False


_WORKDAY_ID = re.compile(
    r"^(?P<tenant>[A-Za-z0-9-]+)\.(?P<environment>wd[0-9]+)"
    r"/(?P<board>[^/]+)$",
    re.IGNORECASE,
)
_WORKDAY_HOST = re.compile(
    r"^(?P<tenant>[A-Za-z0-9-]+)\.(?P<environment>wd[0-9]+)"
    r"\.myworkdayjobs\.com$",
    re.IGNORECASE,
)
_WORKDAY_LOCALE = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$", re.IGNORECASE)
_WORKDAY_CDX_ENVS = ("wd1", "wd3", "wd5", "wd12")
_WORKDAY_CDX_LIMIT = 10_000
_WORKDAY_CDX_MAX_PAGES = 1_000
_WORKDAY_CDX_DELAY_SECONDS = 1.0
_WORKDAY_CDX_PAGE_TIMEOUT = 60
_WORKDAY_CDX_PAGE_RETRIES = 2
_WORKDAY_CDX_MAX_CONSECUTIVE_FAILURES = 3
_WORKDAY_VALIDATION_BATCH_SIZE = 25
_WORKDAY_DETAIL_CONCURRENCY = 8


def _workday_parts(identifier: str) -> tuple[str, str, str]:
    match = _WORKDAY_ID.fullmatch(str(identifier).strip())
    if not match:
        raise ValueError(
            "Workday board identifiers must look like tenant.wd5/Board_Name"
        )
    return (
        match.group("tenant"),
        match.group("environment").lower(),
        urllib.parse.unquote(match.group("board")),
    )


def workday_board_url(identifier: str) -> str:
    """Return the public Workday CXS jobs endpoint for a verified board."""
    tenant, environment, board = _workday_parts(identifier)
    return (
        f"https://{tenant}.{environment}.myworkdayjobs.com/wday/cxs/"
        f"{urllib.parse.quote(tenant, safe='')}/{urllib.parse.quote(board, safe='_-.')}/jobs"
    )


def _workday_job_url(identifier: str, external_path: str) -> str:
    tenant, environment, board = _workday_parts(identifier)
    path = str(external_path or "").strip()
    if not path.startswith("/"):
        path = "/" + path
    return (
        f"https://{tenant}.{environment}.myworkdayjobs.com/en-US/"
        f"{urllib.parse.quote(board, safe='_-.')}{path}"
    )


def _workday_posted_at(value: object) -> str:
    """Convert Workday's ISO or human relative publication label to ISO UTC."""
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")
    except ValueError:
        pass
    lower = text.lower()
    if "today" in lower:
        days = 0
    elif "yesterday" in lower:
        days = 1
    else:
        match = re.search(r"(\d+)\s+days?\s+ago", lower)
        if match:
            days = int(match.group(1))
        elif "30+" in lower:
            days = 30
        else:
            return ""
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(
        timespec="seconds"
    )


def normalize_workday(job: dict) -> dict | None:
    """Normalize a Workday CXS list item into the common posting shape."""
    external_path = str(job.get("externalPath") or "").strip()
    if not external_path or not job.get("title"):
        return None
    bullets = job.get("bulletFields") or []
    requisition = str(job.get("jobPostingId") or (bullets[0] if bullets else "")).strip()
    raw_workplace = str(job.get("remoteType") or "").strip()
    workplace_key = raw_workplace.lower().replace("-", "").replace("_", "")
    if workplace_key in {"telecommute", "remote"}:
        workplace = "remote"
    elif workplace_key in {"flex", "hybrid"}:
        workplace = "hybrid"
    elif workplace_key in {"onsite", "onsiteonly"}:
        workplace = "onsite"
    else:
        workplace = raw_workplace
    return {
        "id": requisition or external_path,
        "title": job.get("title") or "",
        "department": job.get("jobFamily") or "",
        "team": job.get("jobCategory") or "",
        "employmentType": job.get("timeType") or job.get("employmentType") or "",
        "location": job.get("locationsText") or job.get("location") or "",
        "isRemote": workplace == "remote",
        "workplaceType": workplace,
        "address": {
            key: job[key]
            for key in ("locationsText", "primaryLocation", "locations")
            if job.get(key) is not None
        } or None,
        "publishedAt": _workday_posted_at(job.get("postedOn") or job.get("postedAt")),
        "jobUrl": job.get("_jobUrl") or "",
        "_description": job.get("jobDescription") or job.get("description") or "",
    }


def _workday_post_json(url: str, payload: dict, timeout: int = 30) -> dict:
    """POST to the public CXS endpoint with bounded retry handling."""
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    for attempt in range(4):
        try:
            if _is_pooled_host(urllib.parse.urlsplit(url).netloc):
                status, response_headers, response_body = _pooled_request(
                    url, "POST", timeout, headers, body
                )
                if status == 404:
                    raise NotFound(url)
                if status >= 400:
                    if status not in (429, 500, 502, 503, 504):
                        raise urllib.error.HTTPError(
                            url, status, "client error", response_headers, None
                        )
                    retry_after = response_headers.get("retry-after")
                    if attempt == 3:
                        raise urllib.error.HTTPError(
                            url, status, "server error", response_headers, None
                        )
                    time.sleep(_retry_delay(retry_after, attempt))
                    continue
                result = json.loads(response_body)
                if not isinstance(result, dict):
                    raise ValueError("Workday response is not an object")
                return result
            request = urllib.request.Request(
                url, data=body, headers=headers, method="POST"
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                result = json.loads(response.read())
                if not isinstance(result, dict):
                    raise ValueError("Workday response is not an object")
                return result
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise NotFound(url) from exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise
            time.sleep(_retry_delay(exc.headers.get("Retry-After"), attempt))
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError):
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("unreachable")


def _workday_details(url: str) -> dict:
    """Read the public JobPosting JSON-LD embedded in a Workday detail page."""
    html = fetch(url, timeout=20, retries=2).decode("utf-8", "replace")
    match = re.search(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return {}
    try:
        value = json.loads(match.group(1).strip())
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _workday_needs_details(
    item: dict,
    published_after: datetime | None,
) -> bool:
    """Avoid detail requests for list items already outside a cutoff window."""
    if published_after is None:
        return True
    posted = _workday_posted_at(item.get("postedOn") or item.get("postedAt"))
    if not posted:
        return True
    try:
        return datetime.fromisoformat(posted) >= published_after
    except ValueError:
        return True


def _enrich_workday_item(item: dict) -> dict:
    """Best-effort detail enrichment for one cutoff-eligible Workday posting."""
    try:
        details = _workday_details(item["_jobUrl"])
    except Exception:
        return item
    if not details:
        return item
    item["postedAt"] = details.get("datePosted") or item.get("postedOn")
    item["jobDescription"] = details.get("description") or ""
    locations = details.get("jobLocation")
    if isinstance(locations, dict):
        locations = [locations]
    if isinstance(locations, list):
        labels = []
        for location in locations:
            address = (
                location.get("address")
                if isinstance(location, dict)
                else None
            )
            if not isinstance(address, dict):
                continue
            label = ", ".join(
                str(value)
                for value in (
                    address.get("addressLocality"),
                    address.get("addressCountry"),
                )
                if value
            )
            if label and label not in labels:
                labels.append(label)
        if labels:
            item["locationsText"] = "; ".join(labels)
    if details.get("jobLocationType") and not item.get("remoteType"):
        item["remoteType"] = details["jobLocationType"]
    if details.get("employmentType") and not item.get("employmentType"):
        item["employmentType"] = details["employmentType"]
    return item


def _workday_requisition_id(item: dict) -> str:
    bullets = item.get("bulletFields") or []
    return str(item.get("jobPostingId") or (bullets[0] if bullets else "")).strip()


def _apply_cached_workday_item(item: dict, cached: dict) -> None:
    """Reuse persisted detail evidence when a Workday posting is unchanged."""
    published_at = cached.get("published_at")
    if published_at:
        item["postedAt"] = published_at
    if cached.get("description_text"):
        item["jobDescription"] = cached["description_text"]
    if cached.get("location_raw"):
        item["locationsText"] = cached["location_raw"]
    if cached.get("workplace_type"):
        item["remoteType"] = cached["workplace_type"]
    if cached.get("employment_type"):
        item["employmentType"] = cached["employment_type"]


def fetch_workday_jobs(
    identifier: str,
    published_after: datetime | None = None,
    cached_jobs: dict[str, dict] | None = None,
    detail_concurrency: int | None = None,
) -> list[dict]:
    """Fetch paginated Workday postings, stopping at a known publication cutoff."""
    url = workday_board_url(identifier)
    limit = 20
    offset = 0
    all_jobs: list[dict] = []
    cached_jobs = cached_jobs or {}
    enrich_details = published_after is not None or os.environ.get(
        "WORKDAY_ENRICH_DETAILS"
    ) == "1"
    for _ in range(250):
        payload = _workday_post_json(
            url,
            {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": "",
            },
        )
        page = payload.get("jobPostings")
        if not isinstance(page, list):
            raise ValueError(f"workday/{identifier}: response has no jobPostings array")
        page_items = []
        detail_items = []
        for job in page:
            if isinstance(job, dict):
                item = dict(job)
                item["_jobUrl"] = _workday_job_url(
                    identifier, item.get("externalPath", "")
                )
                cached = cached_jobs.get(_workday_requisition_id(item))
                if cached:
                    _apply_cached_workday_item(item, cached)
                page_items.append(item)
                cached_detail = (
                    cached is not None
                    and cached.get("description_text") is not None
                )
                if (
                    enrich_details
                    and not cached_detail
                    and _workday_needs_details(item, published_after)
                ):
                    detail_items.append(item)
        if detail_items:
            with ThreadPoolExecutor(
                max_workers=detail_concurrency or _WORKDAY_DETAIL_CONCURRENCY
            ) as pool:
                list(pool.map(_enrich_workday_item, detail_items))
        all_jobs.extend(page_items)
        total = int(payload.get("total") or 0)
        if len(page) < limit or (total and len(all_jobs) >= total):
            break
        if published_after is not None:
            dated = [
                _workday_posted_at(item.get("postedAt") or item.get("postedOn"))
                for item in page_items
            ]
            parsed = [
                datetime.fromisoformat(value)
                for value in dated
                if value
            ]
            if parsed and max(parsed) < published_after:
                break
        offset += len(page)
    return all_jobs


def inspect_workday_board(
    identifier: str,
    recent_days: int | None = None,
) -> dict:
    """Validate one board and optionally inspect its newest visible posting."""
    try:
        result = _workday_post_json(
            workday_board_url(identifier),
            {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
            timeout=25,
        )
        postings = result.get("jobPostings")
        if not isinstance(postings, list):
            return {
                "identifier": identifier,
                "live": False,
                "outcome": "invalid_response",
                "error": "response did not contain a jobPostings list",
            }
        newest_posted_at = ""
        if recent_days is not None and postings:
            newest = postings[0]
            newest_posted_at = _workday_posted_at(
                newest.get("postedOn") or newest.get("postedAt")
            )
            if not newest_posted_at and newest.get("externalPath"):
                try:
                    details = _workday_details(
                        _workday_job_url(identifier, newest["externalPath"])
                    )
                    newest_posted_at = _workday_posted_at(details.get("datePosted"))
                except Exception:
                    pass
        recent_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=recent_days)
            if recent_days is not None
            else None
        )
        has_recent_job = False
        if newest_posted_at and recent_cutoff is not None:
            try:
                has_recent_job = (
                    datetime.fromisoformat(newest_posted_at) >= recent_cutoff
                )
            except ValueError:
                pass
        return {
            "identifier": identifier,
            "live": True,
            "outcome": "live",
            "totalJobs": int(result.get("total") or len(postings)),
            "newestPostedAt": newest_posted_at or None,
            "hasRecentJob": has_recent_job,
        }
    except NotFound as exc:
        return {
            "identifier": identifier,
            "live": False,
            "outcome": "invalid_response",
            "errorType": type(exc).__name__,
            "error": str(exc),
        }
    except urllib.error.HTTPError as exc:
        # Archived career-site paths are noisy. A missing board or Workday's 422
        # response is a definitive invalid candidate, while access-denied responses
        # can be tenant policy or temporary bot protection and remain retryable.
        outcome = (
            "invalid_response"
            if exc.code in {400, 404, 410, 422}
            else "request_error"
        )
        return {
            "identifier": identifier,
            "live": False,
            "outcome": outcome,
            "errorType": type(exc).__name__,
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "identifier": identifier,
            "live": False,
            "outcome": "request_error",
            "errorType": type(exc).__name__,
            "error": str(exc),
        }


def workday_board_exists(identifier: str) -> bool:
    return bool(inspect_workday_board(identifier).get("live"))


SOURCES = {
    "ashby": {
        "domains": ["jobs.ashbyhq.com"],
        "api": "https://api.ashbyhq.com/posting-api/job-board/{slug}",
        "jobs": lambda payload: payload.get("jobs"),
        "normalize": normalize_ashby,
        # Ashby returns descriptions whether or not we want them.
        "content_param": None,
        "junk_prefixes": ("root.",),
    },
    "greenhouse": {
        "domains": ["boards.greenhouse.io", "job-boards.greenhouse.io"],
        "api": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
        "jobs": lambda payload: payload.get("jobs"),
        "normalize": normalize_greenhouse,
        # Descriptions are opt-in and cost ~26x the bytes (25KB -> 653KB gzipped
        # per board, measured), so they are requested only when --grep needs them.
        "content_param": "content=true",
        "junk_prefixes": (),
    },
    "lever": {
        "domains": ["jobs.lever.co"],
        "api": "https://api.lever.co/v0/postings/{slug}?mode=json",
        # Lever's payload IS the list; there is no wrapper object.
        "jobs": lambda payload: payload if isinstance(payload, list) else None,
        "normalize": normalize_lever,
        "content_param": None,
        "junk_prefixes": (),
    },
    "smartrecruiters": {
        "domains": ["careers.smartrecruiters.com", "jobs.smartrecruiters.com"],
        "api": "https://api.smartrecruiters.com/v1/companies/{slug}/postings",
        "jobs": lambda payload: payload.get("content"),
        "normalize": normalize_smartrecruiters,
        "content_param": None,
        "junk_prefixes": ("api", "assets", "static"),
        "fetch_jobs": fetch_smartrecruiters_jobs,
        "board_exists": smartrecruiters_board_exists,
    },
    "workday": {
        "domains": [
            f"{environment}.myworkdayjobs.com"
            for environment in _WORKDAY_CDX_ENVS
        ],
        "api": None,
        "jobs": lambda payload: payload,
        "normalize": normalize_workday,
        "content_param": None,
        "junk_prefixes": (),
        "fetch_jobs": fetch_workday_jobs,
        "board_exists": workday_board_exists,
    },
    "recruitee": {
        "domains": ["recruitee.com"],
        "registry_only": True,
        "api": None,
        "jobs": lambda payload: payload.get("offers"),
        "normalize": normalize_recruitee,
        "content_param": None,
        "junk_prefixes": (),
        "fetch_jobs": fetch_recruitee_jobs,
        "board_exists": recruitee_board_exists,
    },
    "teamtailor": {
        "domains": ["teamtailor.com"],
        "registry_only": True,
        "api": None,
        "jobs": lambda payload: payload.get("items"),
        "normalize": normalize_teamtailor,
        "content_param": None,
        "junk_prefixes": (),
        "fetch_jobs": fetch_teamtailor_jobs,
        "board_exists": teamtailor_board_exists,
    },
    "workable": {
        "domains": ["apply.workable.com"],
        "registry_only": True,
        "api": None,
        "jobs": lambda payload: payload.get("results"),
        "normalize": normalize_workable,
        "content_param": None,
        "junk_prefixes": (),
        "fetch_jobs": fetch_workable_jobs,
        "board_exists": lambda slug: _workable_board_exists(slug),
    },
}


def _clean(row: dict) -> dict:
    """Trim stray whitespace. Real payloads carry tabs and newlines inside titles,
    which otherwise corrupt sort order and leak into the CSV."""
    return {
        k: (_SPACE.sub(" ", v).strip() if isinstance(v, str) and k != "_description" else v)
        for k, v in row.items()
    }


def _sqlite_value(value: object) -> str:
    """Serialize structured normalized fields for the legacy SQLite export."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def board_url(ats: str, slug: str, want_content: bool = False) -> str:
    if ats == "workday":
        return workday_board_url(slug)
    if ats == "smartrecruiters":
        return smartrecruiters_board_url(slug)
    if ats == "recruitee":
        return recruitee_board_url(slug)
    if ats == "teamtailor":
        return teamtailor_board_url(slug)
    if ats == "workable":
        return workable_board_url(slug)
    url = SOURCES[ats]["api"].format(slug=urllib.parse.quote(slug))
    param = SOURCES[ats]["content_param"]
    if want_content and param:
        url += ("&" if "?" in url else "?") + param
    return url


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def slug_from_url(url: str) -> str | None:
    """First path segment of a board URL, percent-decoded.

    Slugs may contain spaces, e.g. .../A1%20Garage%20Door%20Service/... -> that name.
    """
    path = urllib.parse.urlparse(url).path
    first = path.strip("/").split("/")[0]
    return urllib.parse.unquote(first) or None


def _add(seen: dict[str, str], url: str) -> None:
    """Record the first path segment of a board URL, deduping case-insensitively."""
    slug = slug_from_url(url)
    if slug:
        seen.setdefault(slug.lower(), slug)


def candidates_from_wayback(domains: list[str], since_days: int | None = None) -> dict[str, str]:
    """The Internet Archive's CDX index. Broader than Common Crawl and far more
    reliable — it is the default for that reason.

    `since_days` adds CDX's `from=` filter, which is what makes a daily refresh
    affordable: the last 30 days of Ashby captures is ~6,200 URLs against 191,117
    for the full crawl.
    """
    window = ""
    if since_days is not None:
        start = datetime.now(timezone.utc) - timedelta(days=since_days)
        window = f"&from={start:%Y%m%d}"
    seen: dict[str, str] = {}
    failures = []
    for domain in domains:
        scope = f"last {since_days}d of " if since_days else ""
        print(f"  querying the Wayback Machine for {scope}{domain}...", file=sys.stderr)
        try:
            rows = json.loads(
                fetch(
                    WAYBACK_CDX.format(domain=domain) + window,
                    timeout=300,
                    retries=3,
                )
            )
        except Exception as exc:
            failures.append((domain, exc))
            print(f"    Wayback failed for {domain}: {exc}; continuing", file=sys.stderr)
            continue
        for row in rows[1:]:  # first row is the header
            _add(seen, row[0])
        print(f"    {len(rows) - 1} archived URLs -> {len(seen)} candidates so far",
              file=sys.stderr)
    if not seen and failures:
        raise failures[-1][1]
    return seen


def candidates_from_workday_wayback(
    since_days: int | None = None,
    progress_path: Path | None = WORKDAY_DISCOVERY_PROGRESS,
) -> dict[str, str]:
    """Extract tenant/environment/board identifiers from Workday CDX captures.

    Workday has no public directory. Querying the four stable environment
    domains gives us archived tenant hosts and board paths without probing
    arbitrary company names.
    """
    window = ""
    if since_days is not None:
        start = datetime.now(timezone.utc) - timedelta(days=since_days)
        window = f"&from={start:%Y%m%d}"
    progress = {"window": window, "seen": {}, "environments": {}}
    if progress_path is not None and progress_path.exists():
        try:
            saved = json.loads(progress_path.read_text())
            if saved.get("window") == window:
                progress = saved
                print(
                    f"  resuming Workday discovery from {progress_path.name}",
                    file=sys.stderr,
                )
        except (OSError, json.JSONDecodeError):
            pass
    seen: dict[str, str] = dict(progress.get("seen") or {})

    def save_progress() -> None:
        if progress_path is None:
            return
        progress["seen"] = seen
        progress["updatedAt"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        )
        temporary = progress_path.with_suffix(progress_path.suffix + ".tmp")
        temporary.write_text(json.dumps(progress, indent=2))
        temporary.replace(progress_path)

    for environment in _WORKDAY_CDX_ENVS:
        candidates_before_environment = len(seen)
        pattern = urllib.parse.quote(
            f"{environment}.myworkdayjobs.com/*", safe=""
        )
        base_url = (
            "https://web.archive.org/cdx/search/cdx?"
            f"url={pattern}&matchType=domain&fl=original&collapse=urlkey"
            f"&output=json&filter=statuscode:200&limit={_WORKDAY_CDX_LIMIT}{window}"
        )
        print(
            f"  querying the Wayback Machine for Workday {environment}...",
            file=sys.stderr,
        )
        # showNumPages changes the response field to `numpages`; requesting
        # `fl=original` at the same time makes current CDX return `[null]`.
        # Keep the count query separate or discovery silently reads page one.
        page_count_url = (
            "https://web.archive.org/cdx/search/cdx?"
            f"url={pattern}&matchType=domain&collapse=urlkey&output=json"
            f"&filter=statuscode:200&limit={_WORKDAY_CDX_LIMIT}"
            f"{window}&showNumPages=true"
        )
        environment_progress = progress.setdefault("environments", {}).setdefault(
            environment, {}
        )
        page_count = environment_progress.get("pageCount")
        if page_count is None:
            try:
                page_count_rows = json.loads(
                    fetch(page_count_url, timeout=120, retries=6)
                )
                if (
                    not isinstance(page_count_rows, list)
                    or len(page_count_rows) < 2
                    or page_count_rows[0] != ["numpages"]
                ):
                    raise ValueError(f"unexpected response: {page_count_rows!r}")
                page_count = int(page_count_rows[1][0])
            except Exception as exc:
                save_progress()
                raise RuntimeError(
                    f"Workday {environment} CDX page count failed; "
                    "progress saved for retry"
                ) from exc
            environment_progress["pageCount"] = page_count
            environment_progress["completedPages"] = []
            environment_progress["archivedUrls"] = 0
            save_progress()
        page_count = max(1, page_count)
        if page_count > _WORKDAY_CDX_MAX_PAGES:
            raise RuntimeError(
                f"Workday {environment} has {page_count} CDX pages, above the "
                f"safety limit of {_WORKDAY_CDX_MAX_PAGES}; refusing to truncate"
            )
        archived_urls = int(environment_progress.get("archivedUrls") or 0)
        completed_pages = set(environment_progress.get("completedPages") or [])
        failed_pages = dict(environment_progress.get("failedPages") or {})
        consecutive_failures = 0
        page_urls = [base_url] + [
            f"{base_url}&page={page}" for page in range(1, page_count)
        ]
        for page, url in enumerate(page_urls):
            if page in completed_pages:
                continue
            if completed_pages:
                time.sleep(_WORKDAY_CDX_DELAY_SECONDS)
            print(
                f"    fetching {environment} page {page + 1}/{page_count}...",
                file=sys.stderr,
            )
            try:
                rows = json.loads(
                    fetch(
                        url,
                        timeout=_WORKDAY_CDX_PAGE_TIMEOUT,
                        retries=_WORKDAY_CDX_PAGE_RETRIES,
                    )
                )
            except Exception as exc:
                consecutive_failures += 1
                previous_failure = failed_pages.get(str(page), {})
                failed_pages[str(page)] = {
                    "attempts": int(previous_failure.get("attempts") or 0) + 1,
                    "lastError": f"{type(exc).__name__}: {exc}",
                    "failedAt": datetime.now(timezone.utc).isoformat(
                        timespec="seconds"
                    ),
                }
                environment_progress["failedPages"] = failed_pages
                save_progress()
                print(
                    f"    warning: {environment} page {page + 1}/{page_count} "
                    f"failed; recorded for retry ({consecutive_failures}/"
                    f"{_WORKDAY_CDX_MAX_CONSECUTIVE_FAILURES} consecutive failures)",
                    file=sys.stderr,
                )
                if consecutive_failures >= _WORKDAY_CDX_MAX_CONSECUTIVE_FAILURES:
                    raise RuntimeError(
                        f"Workday {environment} stopped after "
                        f"{consecutive_failures} consecutive CDX page failures; "
                        "progress saved for retry"
                    ) from exc
                continue
            consecutive_failures = 0
            failed_pages.pop(str(page), None)
            archived_urls += max(0, len(rows) - 1)
            for row in rows[1:] if rows else []:
                original = row[0] if isinstance(row, list) and row else row
                parsed = urllib.parse.urlsplit(str(original))
                host = (parsed.hostname or "").lower()
                host_match = _WORKDAY_HOST.fullmatch(host)
                if not host_match:
                    continue
                parts = [
                    urllib.parse.unquote(part)
                    for part in parsed.path.split("/")
                    if part
                ]
                if not parts:
                    continue
                board = ""
                for index, part in enumerate(parts):
                    if part.lower() == "job" and index:
                        board = parts[index - 1]
                        break
                if not board:
                    start = 1 if _WORKDAY_LOCALE.fullmatch(parts[0]) else 0
                    if start < len(parts):
                        board = parts[start]
                if not board or board.lower() in {
                    "job", "wday", "assets", "favicon.ico", "robots.txt", "sitemap.xml"
                }:
                    continue
                identifier = (
                    f"{host_match.group('tenant')}."
                    f"{host_match.group('environment').lower()}/{board}"
                )
                seen.setdefault(identifier.lower(), identifier)
            completed_pages.add(page)
            environment_progress["completedPages"] = sorted(completed_pages)
            environment_progress["archivedUrls"] = archived_urls
            environment_progress["failedPages"] = failed_pages
            save_progress()
            if (page + 1) % 10 == 0 or page + 1 == page_count:
                print(
                    f"    {environment}: {page + 1}/{page_count} pages complete, "
                    f"{len(seen)} unique candidates",
                    file=sys.stderr,
                )
        print(
            f"    {archived_urls} archived URLs across {page_count} CDX pages -> "
            f"{len(seen) - candidates_before_environment} new Workday candidates "
            f"({len(seen)} unique cumulative)",
            file=sys.stderr,
        )
    unresolved = {
        environment: sorted(
            int(page)
            for page in state.get("failedPages", {})
            if int(page) not in set(state.get("completedPages") or [])
        )
        for environment, state in progress.get("environments", {}).items()
    }
    unresolved = {
        environment: pages for environment, pages in unresolved.items() if pages
    }
    if unresolved:
        save_progress()
        count = sum(len(pages) for pages in unresolved.values())
        details = ", ".join(
            f"{environment}: "
            + ", ".join(str(page + 1) for page in pages[:10])
            + ("..." if len(pages) > 10 else "")
            for environment, pages in unresolved.items()
        )
        raise RuntimeError(
            f"Workday CDX crawl has {count} unresolved pages ({details}); "
            "progress saved, rerun the same command to retry only those pages"
        )
    progress["crawlComplete"] = True
    save_progress()
    return seen


def _write_json_atomic(path: Path, payload: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2))
    temporary.replace(path)


def _save_workday_validation_progress(
    validations: dict[str, dict],
) -> None:
    try:
        progress = json.loads(WORKDAY_DISCOVERY_PROGRESS.read_text())
    except (OSError, json.JSONDecodeError):
        progress = {}
    progress["validations"] = validations
    progress["validationUpdatedAt"] = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )
    _write_json_atomic(WORKDAY_DISCOVERY_PROGRESS, progress)


def _cache_verified_workday_boards(identifiers: list[str]) -> int:
    """Add verified-live boards immediately without removing existing boards."""
    if not identifiers:
        return 0
    boards = _read_boards(BOARDS_CACHE)
    workday = boards.setdefault("workday", [])
    known = {identifier.lower() for identifier in workday}
    added = 0
    for identifier in identifiers:
        if identifier.lower() in known:
            continue
        workday.append(identifier)
        known.add(identifier.lower())
        added += 1
    if added:
        workday.sort(key=str.lower)
        _write_json_atomic(BOARDS_CACHE, boards)
    return added


def candidates_from_urlscan(domains: list[str]) -> dict[str, str]:
    """urlscan.io's public scan corpus.

    The Internet Archive is thorough but slow to notice a new board — a median of
    48 days between a board's first posting and its first capture. urlscan indexes
    scans people ran today, so it surfaces boards the archive has not reached yet.
    Sampled once, it found 14 live boards a full Wayback crawl had missed.

    Anonymous use is capped at 30 searches/minute per IP; this makes one per domain.
    A failure here is not fatal — Wayback remains the primary source.
    """
    seen: dict[str, str] = {}
    for domain in domains:
        url = URLSCAN_SEARCH.format(domain=urllib.parse.quote(domain))
        try:
            results = json.loads(fetch(url, timeout=60, retries=2)).get("results", [])
        except Exception as e:
            print(f"  urlscan failed for {domain} ({e}); skipping", file=sys.stderr)
            continue
        for row in results:
            _add(seen, row.get("page", {}).get("url", ""))
        print(f"  urlscan {domain}: {len(results)} scans -> {len(seen)} candidates so far",
              file=sys.stderr)
    return seen


def candidates_from_commoncrawl(domains: list[str], max_pages: int = 20) -> dict[str, str]:
    """Common Crawl's CDX index. Kept as a fallback: narrower coverage, and it
    sheds requests under load often enough to fail for hours at a time."""
    collections = json.loads(fetch(COLLINFO))
    cdx = collections[0]["cdx-api"]
    print(f"  querying Common Crawl index {collections[0]['id']}...", file=sys.stderr)
    seen: dict[str, str] = {}
    for domain in domains:
        query = f"{cdx}?url={urllib.parse.quote(domain)}%2F*&output=json&fl=url"
        # ponytail: walk pages until one comes back empty rather than asking
        # showNumPages first — that query is the most expensive one CDX offers and
        # times out far more often than the pages themselves.
        for page in range(max_pages):
            if page:
                time.sleep(1)  # Common Crawl asks for max 1 CDX request/second.
            try:
                body = fetch(f"{query}&page={page}", timeout=120, retries=6).decode()
            except NotFound:
                break
            if not body.strip():
                break
            for line in body.splitlines():  # JSONL, not a JSON array
                if line.strip():
                    _add(seen, json.loads(line)["url"])
    return seen


def plausible(slug: str, ats: str = "ashby") -> bool:
    """Cheap shape filter, so validation probes thousands of URLs and not millions.

    Archived URLs include tracking blobs, compensation strings and JS fragments as
    "path segments". Every live slug observed is alphanumeric plus space, dot,
    underscore or hyphen; `root.<uuid>` is Ashby's internal embed path, never a board.
    """
    lower = slug.lower()
    return (
        bool(_SLUG_SHAPE.match(slug))
        and lower not in _SLUG_JUNK
        and not lower.startswith(SOURCES.get(ats, {}).get("junk_prefixes", ()))
        and not re.fullmatch(r"[0-9a-f-]{30,}", lower)
    )


def board_exists(ats: str, slug: str) -> bool:
    """Validate a board with the cheapest provider-specific public request.

    The original APIs support cheap HEAD probes. SmartRecruiters uses a one-item GET
    because its public endpoint does not reliably expose the same HEAD behavior.
    """
    try:
        custom = SOURCES[ats].get("board_exists")
        if custom:
            return bool(custom(slug))
        fetch(board_url(ats, slug), timeout=25, retries=2, method="HEAD")
        return True
    except NotFound:
        return False
    except Exception:
        return False  # transient failure: drop it, the next refresh can find it


def discover_boards(
    ats: str,
    concurrency: int = 8,
    recent_days: int | None = None,
    bruteforce: bool = False,
    validate_discovered: bool = False,
) -> list[str]:
    """Find board slugs for one ATS: harvest candidates, then validate each.

    `recent_days` switches to the cheap mode: only archive captures from that window,
    plus urlscan.io, which indexes scans run today rather than waiting on the
    archive's ~48-day median capture lag. Measured at ~4 minutes against ~26 for the
    full crawl, and purely additive: one run added 14 boards and lost none.
    """
    if ats == "workday":
        return discover_workday_boards(
            concurrency=concurrency,
            recent_days=recent_days,
            bruteforce=bruteforce,
            validate_discovered=validate_discovered,
        )
    domains = SOURCES[ats]["domains"]
    print(f"{ats}: discovering boards", file=sys.stderr)
    try:
        seen = candidates_from_wayback(domains, since_days=recent_days)
    except Exception as e:
        print(f"  Wayback failed ({e}); falling back to Common Crawl", file=sys.stderr)
        seen = candidates_from_commoncrawl(domains)
    if recent_days is not None:
        # Additive: urlscan finds boards the archive has not reached, and a failure
        # there must not lose the Wayback results already gathered.
        for key, value in candidates_from_urlscan(domains).items():
            seen.setdefault(key, value)

    candidates = sorted((s for s in seen.values() if plausible(s, ats)), key=str.lower)
    print(f"  validating {len(candidates)} plausible slugs...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        live = [
            s for s, ok in zip(candidates, pool.map(lambda x: board_exists(ats, x), candidates))
            if ok
        ]
    print(f"  {len(live)} live boards ({len(candidates) - len(live)} dead)", file=sys.stderr)

    # Discovery only sees what the archive captured, so a real board that was never
    # crawled is invisible to it. Union in every slug already known-good rather than
    # letting a refresh lose boards an earlier run had.
    known = {s.lower(): s for s in live}
    for path in (BOARDS_SEED, BOARDS_CACHE):
        for slug in _read_boards(path).get(ats, []):
            known.setdefault(slug.lower(), slug)
    if len(known) > len(live):
        print(f"  +{len(known) - len(live)} from seed/previous runs", file=sys.stderr)
    return sorted(known.values(), key=str.lower)


def discover_workday_boards(
    concurrency: int = 8,
    recent_days: int | None = None,
    bruteforce: bool = False,
    validate_discovered: bool = False,
) -> list[str]:
    """Discover and verify Workday boards.

    CDX-derived boards are always checked. Fallback names are deliberately
    opt-in because probing every archived tenant across four environments can
    create thousands of avoidable requests.
    """
    print("workday: discovering boards", file=sys.stderr)
    if validate_discovered:
        try:
            checkpoint = json.loads(WORKDAY_DISCOVERY_PROGRESS.read_text())
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"{WORKDAY_DISCOVERY_PROGRESS.name} does not exist; "
                "run Workday discovery first"
            ) from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{WORKDAY_DISCOVERY_PROGRESS.name} is not valid JSON"
            ) from exc
        seen = dict(checkpoint.get("seen") or {})
        if not seen:
            raise RuntimeError(
                f"{WORKDAY_DISCOVERY_PROGRESS.name} contains no discovered candidates"
            )
        print(
            f"  skipping Wayback; loaded {len(seen)} candidates from "
            f"{WORKDAY_DISCOVERY_PROGRESS.name}",
            file=sys.stderr,
        )
    else:
        try:
            seen = candidates_from_workday_wayback(recent_days)
        except Exception as exc:
            raise RuntimeError(
                "Workday Wayback discovery failed; boards.json was not changed"
            ) from exc

    if bruteforce:
        tenants = {
            (
                identifier.split("/", 1)[0].split(".", 1)[0],
                identifier.split("/", 1)[0].split(".", 1)[1],
            )
            for identifier in seen.values()
            if "/" in identifier and "." in identifier.split("/", 1)[0]
        }
        for tenant, environment in tenants:
            for board in _WORKDAY_FALLBACK_BOARDS:
                identifier = f"{tenant}.{environment}/{board}"
                seen.setdefault(identifier.lower(), identifier)

    candidates = sorted(seen.values(), key=str.lower)
    print(f"  validating {len(candidates)} Workday board candidates...", file=sys.stderr)
    previously_known = {
        identifier.lower()
        for path in (BOARDS_SEED, BOARDS_CACHE)
        for identifier in _read_boards(path).get("workday", [])
    }
    try:
        progress = json.loads(WORKDAY_DISCOVERY_PROGRESS.read_text())
    except (OSError, json.JSONDecodeError):
        progress = {}
    validations: dict[str, dict] = dict(progress.get("validations") or {})
    reclassified = 0
    for status in validations.values():
        if status.get("outcome") != "request_error":
            continue
        error_type = status.get("errorType")
        error = str(status.get("error") or "")
        if error_type == "NotFound" or (
            error_type == "HTTPError"
            and any(f"HTTP Error {code}:" in error for code in (400, 404, 410, 422))
        ):
            status["outcome"] = "invalid_response"
            reclassified += 1
    if reclassified:
        _save_workday_validation_progress(validations)
        print(
            f"  reclassified {reclassified} saved 404/422 responses as invalid; "
            "they will not be retried",
            file=sys.stderr,
        )
    reusable_outcomes = {"live", "invalid_response"}
    pending = [
        identifier
        for identifier in candidates
        if validations.get(identifier.lower(), {}).get("outcome")
        not in reusable_outcomes
    ]
    reused = len(candidates) - len(pending)
    if reused:
        restored = _cache_verified_workday_boards(
            [
                status["identifier"]
                for status in validations.values()
                if status.get("outcome") == "live"
            ]
        )
        print(
            f"  resuming validation: {reused} completed, {len(pending)} pending/retry; "
            f"{restored} verified boards restored to cache",
            file=sys.stderr,
        )
    for start in range(0, len(pending), _WORKDAY_VALIDATION_BATCH_SIZE):
        batch = pending[start:start + _WORKDAY_VALIDATION_BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            batch_statuses = list(
                pool.map(
                    lambda identifier: inspect_workday_board(
                        identifier, RECENT_WINDOW_DAYS
                    ),
                    batch,
                )
            )
        checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for status in batch_statuses:
            key = status["identifier"].lower()
            previous = validations.get(key, {})
            validations[key] = {
                **status,
                "checkedAt": checked_at,
                "attempts": int(previous.get("attempts") or 0) + 1,
            }
        added = _cache_verified_workday_boards(
            [status["identifier"] for status in batch_statuses if status["live"]]
        )
        _save_workday_validation_progress(validations)
        completed = reused + min(start + len(batch), len(pending))
        print(
            f"    validation: {completed}/{len(candidates)} complete; "
            f"{added} newly verified boards saved",
            file=sys.stderr,
        )
    statuses = [validations[identifier.lower()] for identifier in candidates]
    live = [status["identifier"] for status in statuses if status["live"]]
    invalid = [
        status for status in statuses if status.get("outcome") == "invalid_response"
    ]
    request_errors = [
        status for status in statuses if status.get("outcome") == "request_error"
    ]
    recent = [
        status["identifier"]
        for status in statuses
        if status["live"] and status.get("hasRecentJob")
    ]
    newly_cached = [
        identifier for identifier in live if identifier.lower() not in previously_known
    ]

    known = {identifier.lower(): identifier for identifier in live}
    for path in (BOARDS_SEED, BOARDS_CACHE):
        for identifier in _read_boards(path).get("workday", []):
            known.setdefault(identifier.lower(), identifier)
    live_keys = {identifier.lower() for identifier in live}
    retained_only = [
        identifier
        for key, identifier in known.items()
        if key not in live_keys
    ]
    omitted_live = [
        identifier for identifier in live if identifier.lower() not in known
    ]
    if omitted_live:
        raise RuntimeError(
            "Workday discovery refused to write an incomplete board list; "
            f"{len(omitted_live)} validated live boards were omitted: "
            + ", ".join(omitted_live)
        )
    expected_known_count = len(live_keys) + len(retained_only)
    if len(known) != expected_known_count:
        raise RuntimeError(
            "Workday discovery accounting mismatch: "
            f"{len(live_keys)} live + {len(retained_only)} retained != "
            f"{len(known)} output boards"
        )
    print(
        f"  candidate accounting: {len(candidates)} checked = {len(live)} live + "
        f"{len(invalid)} invalid responses + {len(request_errors)} request errors",
        file=sys.stderr,
    )
    print(
        f"  boards.json accounting: {len(known)} output = {len(live_keys)} "
        f"validated live + {len(retained_only)} retained from seed/cache; "
        f"{len(recent)} live boards have a job in the last {RECENT_WINDOW_DAYS} days; "
        f"{len(newly_cached)} newly cached",
        file=sys.stderr,
    )
    _write_json_atomic(
        WORKDAY_DISCOVERY_REPORT,
        {
                "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "candidateCount": len(candidates),
                "liveCount": len(live),
                "invalidResponseCount": len(invalid),
                "requestErrorCount": len(request_errors),
                "recentCount": len(recent),
                "newlyCachedCount": len(newly_cached),
                "retainedCount": len(retained_only),
                "boards": [
                    {
                        **status,
                        "previouslyKnown": (
                            status["identifier"].lower() in previously_known
                        ),
                        "newlyCached": status["identifier"] in newly_cached,
                        "retainedWithoutRediscovery": False,
                    }
                    for status in statuses
                ] + [
                    {
                        "identifier": identifier,
                        "live": None,
                        "outcome": "retained_without_rediscovery",
                        "totalJobs": None,
                        "newestPostedAt": None,
                        "hasRecentJob": None,
                        "previouslyKnown": True,
                        "newlyCached": False,
                        "retainedWithoutRediscovery": True,
                    }
                    for identifier in retained_only
                ],
        },
    )
    print(
        f"  discovery audit -> {WORKDAY_DISCOVERY_REPORT.name}",
        file=sys.stderr,
    )
    if request_errors:
        raise RuntimeError(
            f"Workday validation had {len(request_errors)} request errors; "
            f"{len(live)} live boards were verified and {len(newly_cached)} were "
            f"added to {BOARDS_CACHE.name}; "
            f"{WORKDAY_DISCOVERY_PROGRESS.name} was retained so only the request "
            "errors need retrying"
        )
    return sorted(known.values(), key=str.lower)


def _read_boards(path: Path) -> dict[str, list[str]]:
    """Read a board file, accepting the pre-multi-ATS flat list as Ashby."""
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {"ashby": data} if isinstance(data, list) else data


RECENT_WINDOW_DAYS = 30


def load_boards(
    refresh: bool,
    ats_list: list[str],
    concurrency: int = 8,
    recent: bool = False,
    workday_bruteforce: bool = False,
    validate_discovered: bool = False,
) -> dict[str, list[str]]:
    if not refresh and not recent:
        # Merge per platform rather than taking the first file that has anything.
        # Keep cache order for stable imports, then append verified seed boards that
        # were discovered after the cache was generated.
        merged: dict[str, list[str]] = {}
        for path in (BOARDS_CACHE, BOARDS_SEED):
            for ats, slugs in _read_boards(path).items():
                if not slugs:
                    continue
                existing = {slug.lower() for slug in merged.setdefault(ats, [])}
                for slug in slugs:
                    if slug.lower() not in existing:
                        merged[ats].append(slug)
                        existing.add(slug.lower())
        got = {a: merged.get(a, []) for a in ats_list}
        if any(got.values()):
            summary = ", ".join(f"{a} {len(v)}" for a, v in got.items())
            print(f"{sum(len(v) for v in got.values())} boards ({summary})",
                  file=sys.stderr)
            for ats, slugs in got.items():
                if not slugs:
                    print(f"  note: no {ats} boards cached; run --refresh-boards",
                          file=sys.stderr)
            return got
    boards = _read_boards(BOARDS_CACHE)
    for ats in ats_list:
        if SOURCES[ats].get("registry_only"):
            retained = boards.get(ats, [])
            if retained:
                print(
                    f"{ats}: retaining {len(retained)} registry-validated boards; "
                    "archive discovery is disabled for this provider",
                    file=sys.stderr,
                )
            else:
                print(
                    f"{ats}: no registry-validated boards are cached",
                    file=sys.stderr,
                )
            continue
        try:
            boards[ats] = discover_boards(
                ats,
                concurrency,
                recent_days=RECENT_WINDOW_DAYS if recent else None,
                bruteforce=workday_bruteforce if ats == "workday" else False,
                validate_discovered=validate_discovered if ats == "workday" else False,
            )
        except RateLimited:
            sys.exit(
                "Common Crawl returned 503: request rate too high. Their docs say to "
                "slow down, and that a repeatedly-abusive IP can be blocked for 24 "
                "hours. Wait before retrying."
            )
        except (urllib.error.URLError, TimeoutError) as e:
            sys.exit(
                f"board discovery failed for {ats}: {e}\n"
                "Both the Wayback Machine and Common Crawl were unreachable. Retry "
                "later; the bundled boards.seed.json means this phase is optional."
            )
    _write_json_atomic(BOARDS_CACHE, boards)
    if "workday" in ats_list and not validate_discovered:
        try:
            workday_progress = json.loads(WORKDAY_DISCOVERY_PROGRESS.read_text())
        except (OSError, json.JSONDecodeError):
            workday_progress = {}
        if workday_progress.get("crawlComplete"):
            WORKDAY_DISCOVERY_PROGRESS.unlink(missing_ok=True)
    total = sum(len(boards.get(a, [])) for a in ats_list)
    print(f"cached {total} slugs -> {BOARDS_CACHE.name}", file=sys.stderr)
    return {a: boards.get(a, []) for a in ats_list}


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #


_DURATION = re.compile(r"^(\d+)\s*([dwmy]?)$", re.IGNORECASE)
_DURATION_DAYS = {"d": 1, "w": 7, "m": 30, "y": 365, "": 1}


def parse_duration(text: str) -> int:
    """'7d' / '2w' / '3m' / '1y' / '7' -> days. Raises ValueError on anything else."""
    m = _DURATION.match(text.strip())
    if not m:
        raise ValueError(f"expected something like 7d, 2w, 3m or 90; got {text!r}")
    return int(m.group(1)) * _DURATION_DAYS[m.group(2).lower()]


def published_within(published_at: str, cutoff: datetime) -> bool:
    """Is this posting newer than the cutoff?

    An unparseable or missing date counts as too old. Every one of 308,100 rows
    measured had a usable date, so this only guards against a future API change —
    and excluding is the safe direction, since --since exists to promise freshness.
    """
    if not published_at:
        return False
    try:
        return datetime.fromisoformat(published_at.replace("Z", "+00:00")) >= cutoff
    except ValueError:
        return False


def matches(job_title: str, wanted: str, mode: str = "fuzzy") -> bool:
    """Does a posting's title match what the user asked for? Case-insensitive.

    exact  — the whole title equals the query.
             "software engineer" matches "Software Engineer" only.
    fuzzy  — either string contains the other, so it works in both directions:
             a short query finds longer titles ("software engineer" ->
             "Senior Software Engineer, Backend") and a long query still finds
             the short title it contains ("senior software engineer, backend"
             -> "Software Engineer").

             The reverse direction requires the title to be at least two words.
             Without that, querying "senior software engineer" also matches jobs
             titled just "Engineer", "Software", or "Senior" — every one-word
             title that happens to appear in the query.
    """
    title, want = job_title.lower().strip(), wanted.lower().strip()
    if not title or not want:
        return False  # an empty query would otherwise match every job
    if mode == "exact":
        return title == want
    return want in title or (len(title.split()) >= 2 and title in want)


def fragments(text: str, pattern: re.Pattern[str], limit: int = 2) -> list[str]:
    """Windows of surrounding text for each match, so a hit can be judged in context."""
    found: list[str] = []
    for match in pattern.finditer(text):
        window = text[max(0, match.start() - 90) : match.end() + 150].strip()
        if window not in found:
            found.append(window)
        if len(found) == limit:
            break
    return found


def scan_board(
    ats: str,
    slug: str,
    wanted: str | None,
    remote_only: bool,
    mode: str,
    pattern: re.Pattern[str] | None = None,
    cutoff: datetime | None = None,
    etag: str | None = None,
    meta: dict | None = None,
) -> list[dict]:
    """Fetch one board, return flat rows for matching listed jobs.

    Descriptions are read only when --grep needs them, and even then only the
    matched fragments survive — the full text dominates every payload, and holding
    thousands of boards' worth would be gigabytes. --grep matches against the title
    as well as the description; see the loop below for why they are searched apart.
    """
    source = SOURCES[ats]
    if source.get("fetch_jobs"):
        jobs = source["fetch_jobs"](slug, cutoff)
    else:
        payload = json.loads(
            fetch(
                board_url(ats, slug, want_content=pattern is not None),
                etag=etag,
                meta=meta,
            )
        )
        jobs = source["jobs"](payload)
    if not isinstance(jobs, list):
        # Fail loudly on a shape change rather than silently reporting no results.
        raise ValueError(f"{ats}/{slug}: response has no jobs array")

    rows = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        norm = source["normalize"](job)
        if norm is None:
            continue
        if ats in {
            "smartrecruiters",
            "workday",
            "recruitee",
            "teamtailor",
            "workable",
        }:
            norm["id"] = f"{slug}:{norm['id']}"
        if ats == "workable" and not norm.get("jobUrl"):
            norm["jobUrl"] = workable_job_url(slug, str(job.get("shortcode") or norm["id"]))
        norm = _clean(norm)
        if cutoff is not None and not published_within(norm["publishedAt"], cutoff):
            continue
        if wanted and not matches(norm["title"], wanted, mode):
            continue
        if remote_only and not norm["isRemote"]:
            continue

        hits: list[str] = []
        if pattern is not None:
            # The title is searched too. Searching only the description dropped
            # postings whose subject is *in the title* — "SSO Integrations Lead",
            # "Identity Platform Engineer" — whenever the body happened to phrase
            # it differently. Searched separately rather than concatenated, or a
            # regex could match across the seam and report a hit in neither field.
            hits = fragments(plain_text(norm["title"]), pattern, limit=1)
            for window in fragments(plain_text(norm["_description"]), pattern):
                if window not in hits:
                    hits.append(window)
            if not hits:
                continue

        norm.pop("_description", None)
        rows.append({"ats": ats, "company": slug, **norm, "matched": " … ".join(hits)})
    return rows


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #

_INDEXES = """
CREATE INDEX IF NOT EXISTS jobs_company ON jobs(ats, company);
CREATE INDEX IF NOT EXISTS jobs_last_seen ON jobs(last_seen);
CREATE INDEX IF NOT EXISTS jobs_closed_at ON jobs(closed_at);
"""


def _create_table(con: sqlite3.Connection, name: str = "jobs") -> None:
    # (ats, id) rather than id alone: Greenhouse ids are integers while Ashby and
    # Lever use UUIDs, so a bare id risks a collision that would silently overwrite
    # one platform's posting with another's.
    body = "".join(f"{f} TEXT," for f in FIELDS if f not in ("ats", "id"))
    con.executescript(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            ats         TEXT NOT NULL,
            id          TEXT NOT NULL,
            {body}
            first_seen  TEXT NOT NULL,
            last_seen   TEXT NOT NULL,
            closed_at   TEXT,
            PRIMARY KEY (ats, id)
        );
    """)


def _prepare(con: sqlite3.Connection) -> None:
    """Create the table, migrate an older one, then index — in that order.

    Indexes come last because an index on a column the migration has not added yet
    cannot be created; putting them in the same script as CREATE TABLE is what broke
    the previous migration.
    """
    cols = {c[1] for c in con.execute("PRAGMA table_info(jobs)")}
    if cols and "ats" not in cols:
        # Pre-multi-ATS database. The primary key is changing, which ALTER TABLE
        # cannot do, so rebuild and label every existing row as Ashby.
        if "closed_at" not in cols:
            con.execute("ALTER TABLE jobs ADD COLUMN closed_at TEXT")
            cols.add("closed_at")
        carried = [
            c for c in (*FIELDS, "first_seen", "last_seen", "closed_at")
            if c in cols and c != "ats"
        ]
        _create_table(con, "jobs_new")
        con.execute(
            f"INSERT INTO jobs_new (ats, {','.join(carried)}) "
            f"SELECT 'ashby', {','.join(carried)} FROM jobs"
        )
        con.execute("DROP TABLE jobs")
        con.execute("ALTER TABLE jobs_new RENAME TO jobs")
    else:
        _create_table(con)
        if cols and "closed_at" not in cols:
            con.execute("ALTER TABLE jobs ADD COLUMN closed_at TEXT")
    con.executescript(_INDEXES)


def sort_rows(rows: list[dict], mode: str) -> None:
    """Order rows in place. `recent` puts the newest posting first.

    Comparing the ISO strings is correct without parsing, because every adapter
    normalises to ISO — including Lever's epoch milliseconds. Two passes rather than
    one compound key: Python's sort is stable, so sorting by board first and then by
    date gives newest-first with a deterministic order inside each timestamp. An
    empty date is the smallest string, so reversing puts undated rows last.
    """
    rows.sort(key=lambda r: (r["ats"], str(r["company"]).lower(), str(r["title"]).lower()))
    if mode == "recent":
        rows.sort(key=lambda r: str(r["publishedAt"]), reverse=True)


def may_close_postings(
    title: str | None,
    pattern: re.Pattern[str] | None,
    cutoff: datetime | None,
    new_only: bool,
) -> bool:
    """Did this run see every posting on the boards it scanned?

    Only such a run may stamp closed_at. Every filter has to be listed here: a run
    that skipped old postings, or ones it had seen before, did not observe them and
    cannot conclude they are gone. Miss one and, for example, `--all --since 7d`
    would mark every posting older than a week as closed.
    """
    return not (title or pattern or cutoff or new_only)


_ETAG_SCHEMA = """
CREATE TABLE IF NOT EXISTS board_etag (
    ats     TEXT NOT NULL,
    company TEXT NOT NULL,
    etag    TEXT NOT NULL,
    seen_at TEXT NOT NULL,
    PRIMARY KEY (ats, company)
);
"""


def may_use_etags(
    title: str | None,
    pattern: re.Pattern[str] | None,
    cutoff: datetime | None,
    remote_only: bool,
    new_only: bool,
) -> bool:
    """Is a 304 safe to treat as "nothing new on this board"?

    Only for a run that is unfiltered apart from --new-only. A 304 says the body is
    unchanged since the stored etag; concluding "no new postings" from that also
    requires that the fetch which stored the etag actually persisted every posting.
    A --title run stores rows for matching postings only, so trusting its etag later
    would skip a board whose non-matching postings were never recorded.

    Storing and using etags are gated on the same predicate, so an etag in the
    database always came from a full, persisted fetch.
    """
    return new_only and not (title or pattern or cutoff or remote_only)


def load_etags(db_path: Path) -> dict[tuple[str, str], str]:
    """Stored etags, keyed by board. Empty if the table does not exist yet."""
    if not db_path.exists():
        return {}
    with sqlite3.connect(db_path) as con:
        con.executescript(_ETAG_SCHEMA)
        return {(a, c): e for a, c, e in con.execute(
            "SELECT ats, company, etag FROM board_etag")}


def save_etags(db_path: Path, etags: dict[tuple[str, str], str], seen_at: str) -> None:
    if not etags:
        return
    with sqlite3.connect(db_path) as con:
        con.executescript(_ETAG_SCHEMA)
        con.executemany(
            "INSERT INTO board_etag (ats, company, etag, seen_at) VALUES (?,?,?,?) "
            "ON CONFLICT(ats, company) DO UPDATE SET etag=excluded.etag, "
            "seen_at=excluded.seen_at",
            [(a, c, e, seen_at) for (a, c), e in etags.items()],
        )


def known_keys(db_path: Path) -> set[tuple[str, str]]:
    """The (ats, id) pairs already recorded. Empty set if the database is new."""
    if not db_path.exists():
        return set()
    with sqlite3.connect(db_path) as con:
        if not con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone():
            return set()
        cols = {c[1] for c in con.execute("PRAGMA table_info(jobs)")}
        if "ats" not in cols:  # pre-multi-ATS database; everything in it is Ashby
            return {("ashby", r[0]) for r in con.execute("SELECT id FROM jobs")}
        return {tuple(r) for r in con.execute("SELECT ats, id FROM jobs")}


def save(
    rows: list[dict],
    db_path: Path,
    seen_at: str,
    covered: list[tuple[str, str]] | None = None,
) -> tuple[int, int, int]:
    """Upsert rows keyed on (ats, posting id). Returns (new, updated, closed).

    first_seen is preserved across runs and last_seen is refreshed, which is the
    whole reason to keep a database rather than just the CSV: it answers "when did
    this posting appear" and "is it still up" across scrapes.

    `covered` is the list of (ats, board) pairs this run scanned exhaustively, and is
    only passed for an unfiltered run. On a filtered run a missing job is ambiguous —
    it may be gone, or it may simply not have matched --title — so only an unfiltered
    run has the standing to close a posting. Anything on a covered board that this run
    did not see is stamped closed_at; anything that reappears has it cleared.
    """
    keyed = {(r["ats"], r["id"]): r for r in rows if r.get("id")}
    cols = ["ats", "id", *[f for f in FIELDS if f not in ("ats", "id")]]
    with sqlite3.connect(db_path) as con:
        _prepare(con)
        known = {tuple(r) for r in con.execute("SELECT ats, id FROM jobs")}
        con.executemany(
            f"INSERT INTO jobs ({','.join(cols)}, first_seen, last_seen) "
            f"VALUES ({','.join('?' * len(cols))}, ?, ?) "
            "ON CONFLICT(ats, id) DO UPDATE SET "
            # Everything except first_seen is refreshed; titles and locations do
            # get edited in place on live postings. `matched` is the exception: it
            # belongs to whichever --grep produced it, so a later title-only run
            # must not blank out context an earlier search found.
            + ",".join(
                f"{c}=excluded.{c}" for c in cols if c not in ("ats", "id", "matched")
            )
            + ", matched=CASE WHEN excluded.matched != '' "
            "THEN excluded.matched ELSE jobs.matched END"
            ", last_seen=excluded.last_seen",
            [
                [_sqlite_value(r.get(c, "")) for c in cols] + [seen_at, seen_at]
                for r in keyed.values()
            ],
        )
        closed = 0
        if covered is not None:
            con.execute(
                "CREATE TEMP TABLE scanned (ats TEXT, company TEXT, "
                "PRIMARY KEY (ats, company))"
            )
            con.executemany("INSERT OR IGNORE INTO scanned VALUES (?, ?)", covered)
            cur = con.execute(
                "UPDATE jobs SET closed_at = ? "
                "WHERE closed_at IS NULL AND last_seen < ? "
                "AND (ats, company) IN (SELECT ats, company FROM scanned)",
                (seen_at, seen_at),
            )
            closed = cur.rowcount
            # A posting that came back is open again.
            con.execute(
                "UPDATE jobs SET closed_at = NULL "
                "WHERE closed_at IS NOT NULL AND last_seen = ?",
                (seen_at,),
            )

    new = len(keyed.keys() - known)
    return new, len(keyed) - new, closed


# --------------------------------------------------------------------------- #


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--ats",
        default="all",
        help="comma-separated platforms: " + ", ".join(SOURCES) + " (default: all)",
    )
    p.add_argument(
        "--title",
        help="title to match (default: 'software engineer', unless --grep is given)",
    )
    p.add_argument(
        "--match",
        choices=("fuzzy", "exact"),
        default="fuzzy",
        help="fuzzy: either string contains the other (default). exact: titles must be equal",
    )
    p.add_argument(
        "--grep",
        metavar="REGEX",
        help="case-insensitive regex searched against the job title and description; "
        "matching context lands in the 'matched' column. On Greenhouse this "
        "requests full content, which is ~26x the bytes",
    )
    p.add_argument(
        "--all",
        action="store_true",
        help="every listed job on every board, no title or description filter",
    )
    p.add_argument(
        "--since",
        metavar="AGE",
        help="only postings published within this window: 7d, 2w, 3m, 1y, or a bare "
        "number of days. Across all platforms the median posting is 62 days old; "
        "--since 7d returns ~11%% of them at a median age of 4 days",
    )
    p.add_argument(
        "--new-only",
        action="store_true",
        help="only postings the database has never seen. Catches an old requisition "
        "that appeared today, which --since cannot. Requires the database",
    )
    p.add_argument(
        "--sort",
        choices=("board", "recent"),
        default="board",
        help="board: grouped by platform and company (default). recent: newest "
        "posting first, which is what you want with --since",
    )
    p.add_argument("--limit", type=int, help="max boards per platform (default: all)")
    p.add_argument("--remote", action="store_true", help="only remote postings")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--refresh-boards", action="store_true", help="re-crawl slug lists")
    p.add_argument(
        "--discover-only",
        action="store_true",
        help="refresh boards.json and exit without fetching job postings; use with "
        "--refresh-boards or --refresh-recent",
    )
    p.add_argument(
        "--validate-discovered",
        action="store_true",
        help="validate Workday candidates already saved in the discovery checkpoint "
        "without contacting Wayback; use with --ats workday --discover-only",
    )
    p.add_argument(
        "--workday-bruteforce",
        action="store_true",
        help="when discovering Workday, also probe fallback board names for archived tenants",
    )
    p.add_argument(
        "--refresh-recent",
        action="store_true",
        help="cheap daily discovery: urlscan.io plus the last 30 days of archive "
        "captures. ~4 minutes against the full crawl's ~26",
    )
    p.add_argument("--out", default="job-boards", help="output filename prefix")
    p.add_argument(
        "--boards-from",
        metavar="FILE",
        help="scan only the boards in this file instead of the discovered list. "
        "Takes the same shape as boards.json, which is what <out>.failed.json is "
        "written in, so retrying a run's failures is --boards-from <out>.failed.json",
    )
    p.add_argument(
        "--db",
        default="job-boards.db",
        help="SQLite file accumulating every scrape (default: job-boards.db)",
    )
    p.add_argument("--no-db", action="store_true", help="skip the database write")
    args = p.parse_args()

    ats_list = list(SOURCES) if args.ats == "all" else [
        a.strip() for a in args.ats.split(",") if a.strip()
    ]
    unknown = [a for a in ats_list if a not in SOURCES]
    if unknown:
        sys.exit(f"unknown --ats {', '.join(unknown)}; choose from {', '.join(SOURCES)}")

    if args.all and (args.title or args.grep):
        sys.exit("--all takes no filters; drop --title/--grep or drop --all")
    if args.discover_only:
        if (
            args.all
            or args.title
            or args.grep
            or args.since
            or args.new_only
            or args.limit is not None
            or args.remote
            or args.boards_from
            or args.no_db
        ):
            sys.exit(
                "--discover-only cannot be combined with scrape filters, --limit, "
                "--boards-from, or --no-db"
            )
        if not args.refresh_boards and not args.refresh_recent and not args.validate_discovered:
            sys.exit(
                "--discover-only requires --refresh-boards, --refresh-recent, "
                "or --validate-discovered"
            )
    if args.validate_discovered:
        if not args.discover_only:
            sys.exit("--validate-discovered requires --discover-only")
        if ats_list != ["workday"]:
            sys.exit("--validate-discovered requires --ats workday")
        if args.refresh_boards or args.refresh_recent:
            sys.exit(
                "--validate-discovered cannot be combined with "
                "--refresh-boards or --refresh-recent"
            )
    if args.new_only and args.no_db:
        sys.exit("--new-only compares against the database; it cannot be used with --no-db")
    cutoff = None
    if args.since:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=parse_duration(args.since))
        except ValueError as e:
            sys.exit(f"--since: {e}")
    # The title default only applies when nothing else narrows the search. Applying
    # it to a --grep run would silently AND an unrequested title filter onto it.
    title = None if args.all else (
        args.title or (None if args.grep else "software engineer")
    )
    try:
        pattern = re.compile(args.grep, re.IGNORECASE) if args.grep else None
    except re.error as e:
        sys.exit(f"--grep is not a valid regex: {e}")
    if args.grep and r"\b" not in args.grep:
        # Silent and severe: `rust` matches "trust", which appears in almost every
        # description's boilerplate. Measured 1350 hits vs 72 word-bounded.
        print(
            rf"note: --grep {args.grep!r} has no \b word boundary, so it matches "
            rf"inside longer words. Consider '\b{args.grep}\b'.",
            file=sys.stderr,
        )
    if pattern is not None and "greenhouse" in ats_list:
        print(
            "note: --grep requests full descriptions from Greenhouse, roughly 26x "
            "the bytes of a normal run.",
            file=sys.stderr,
        )

    if args.discover_only:
        boards = load_boards(
            True,
            ats_list,
            args.concurrency,
            recent=args.refresh_recent,
            workday_bruteforce=args.workday_bruteforce,
            validate_discovered=args.validate_discovered,
        )
        print(
            (
                "saved partial Workday validation"
                if args.validate_discovered
                else "board discovery complete"
            )
            + f": {sum(len(values) for values in boards.values())} "
            f"boards cached in {BOARDS_CACHE.name}"
        )
        return

    if args.boards_from:
        source = Path(args.boards_from)
        if not source.is_absolute():
            source = HERE / source
        boards = _read_boards(source)
        if not boards:
            sys.exit(f"--boards-from {args.boards_from}: no boards in that file")
    else:
        boards = load_boards(
            args.refresh_boards,
            ats_list,
            args.concurrency,
            recent=args.refresh_recent,
            workday_bruteforce=args.workday_bruteforce,
        )
    scanned = [
        (ats, slug)
        for ats in ats_list
        for slug in (boards.get(ats, [])[: args.limit] if args.limit else boards.get(ats, []))
    ]
    criteria = [f"title {title!r} ({args.match})" if title else "",
                f"title or description /{args.grep}/" if args.grep else "",
                f"published within {args.since}" if args.since else "",
                "unseen postings only" if args.new_only else ""]
    what = " + ".join(c for c in criteria if c) or "every listed job"
    print(f"scanning {len(scanned)} boards across {len(ats_list)} platforms "
          f"for {what}...", file=sys.stderr)

    rows: list[dict] = []
    dead: set[tuple[str, str]] = set()
    # Which boards failed, not just how many. A count on stderr left no way to
    # re-scan the survivors of a throttle without repeating all 13,000 boards.
    # list.append is atomic under the GIL, so this needs no lock — same as `dead`.
    failed: list[tuple[str, str]] = []

    # Conditional requests, but only when a 304 genuinely means "nothing new here".
    db_path = HERE / args.db if not Path(args.db).is_absolute() else Path(args.db)
    conditional = not args.no_db and may_use_etags(
        title, pattern, cutoff, args.remote, args.new_only
    )
    etags = load_etags(db_path) if conditional else {}
    fresh_etags: dict[tuple[str, str], str] = {}
    unchanged = 0
    etag_lock = threading.Lock()
    if conditional and etags:
        print(f"  {len(etags)} boards have a stored etag; unchanged ones will be skipped",
              file=sys.stderr)

    def work(item: tuple[str, str]) -> list[dict]:
        nonlocal unchanged
        ats, slug = item
        meta: dict = {}
        for attempt in range(2):
            try:
                found = scan_board(
                    ats, slug, title, args.remote, args.match, pattern, cutoff,
                    etag=etags.get(item) if conditional else None,
                    meta=meta if conditional else None,
                )
                if conditional and meta.get("etag"):
                    with etag_lock:
                        fresh_etags[item] = meta["etag"]
                return found
            except NotModified:
                with etag_lock:
                    unchanged += 1
                return []
            except NotFound:
                dead.add(item)
                return []
            except Exception as e:
                if attempt:
                    failed.append(item)
                    print(f"  ! {ats}/{slug}: {e}", file=sys.stderr)
        return []

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for i, found in enumerate(pool.map(work, scanned), 1):
            rows.extend(found)
            if i % 500 == 0 or i == len(scanned):
                print(
                    f"  {i}/{len(scanned)} boards | {len(dead)} 404 | "
                    f"{len(failed)} err | {unchanged} unchanged | {len(rows)} matches",
                    file=sys.stderr,
                )

    if args.new_only:
        # Drop anything the database has already recorded. Done here rather than in
        # scan_board so the board fetch stays independent of storage.
        before = len(rows)
        seen_before = known_keys(db_path)
        rows = [r for r in rows if (r["ats"], r["id"]) not in seen_before]
        print(f"  --new-only: {before - len(rows)} already known, {len(rows)} new",
              file=sys.stderr)

    sort_rows(rows, args.sort)

    # BOM so Excel renders the en-dashes and bullets in location strings.
    csv_path = HERE / f"{args.out}.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    json_path = HERE / f"{args.out}.json"
    json_path.write_text(json.dumps(rows, indent=2))

    # Boards that errored, in the same shape as boards.json, so the retry is just
    # `--boards-from <out>.failed.json`. Written only when something failed, and
    # removed otherwise so a stale file from an earlier run cannot be re-read as
    # if it described this one. 404s are excluded: a dead slug is an expected
    # answer, not a failure, and it is already self-pruned below.
    failed_path = HERE / f"{args.out}.failed.json"
    if failed:
        by_platform: dict[str, list[str]] = {}
        for ats, slug in failed:
            by_platform.setdefault(ats, []).append(slug)
        failed_path.write_text(json.dumps(by_platform, indent=2))
        print(f"  {len(failed)} board{'s' if len(failed) != 1 else ''} failed -> "
              f"{failed_path.name} (retry with --boards-from {failed_path.name})",
              file=sys.stderr)
    elif failed_path.exists():
        failed_path.unlink()

    # Self-prune: drop slugs that 404'd so later runs skip them. Skipped for
    # --boards-from as well as --limit: `boards` is then a caller-supplied subset,
    # and with no cache on disk to fall back to it would be written out as though
    # it were the whole discovered board list.
    if dead and not args.limit and not args.boards_from:
        cached = _read_boards(BOARDS_CACHE) or boards
        for ats in ats_list:
            cached[ats] = [s for s in cached.get(ats, []) if (ats, s) not in dead]
        BOARDS_CACHE.write_text(json.dumps(cached, indent=2))

    written = f"{csv_path.name}, {json_path.name}"
    if not args.no_db:
        seen_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if conditional:
            save_etags(db_path, fresh_etags, seen_at)
        # Only an unfiltered run saw everything, so only it may close postings.
        covered = scanned if may_close_postings(title, pattern, cutoff, args.new_only) else None
        new, updated, closed = save(rows, db_path, seen_at, covered)
        written += f", {db_path.name} ({new} new, {updated} already seen"
        written += f", {closed} closed)" if covered is not None else ")"

    by_ats = {a: sum(1 for r in rows if r["ats"] == a) for a in ats_list}
    print(f"\n{len(rows)} jobs ({', '.join(f'{a} {n}' for a, n in by_ats.items())}) "
          f"-> {written}", file=sys.stderr)


if __name__ == "__main__":
    main()
