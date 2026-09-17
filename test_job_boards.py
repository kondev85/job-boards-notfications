#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Offline self-check. Run: uv run test_job_boards.py  (or python3 test_job_boards.py)"""

import contextlib
import csv
import io
from datetime import datetime, timezone
import json
import re
import sqlite3
import tempfile
from pathlib import Path

from job_boards import (
    FIELDS,
    board_url,
    fragments,
    matches,
    normalize_ashby,
    normalize_greenhouse,
    normalize_lever,
    normalize_smartrecruiters,
    normalize_recruitee,
    normalize_teamtailor,
    normalize_workable,
    normalize_workday,
    plain_text,
    plausible,
    save,
    scan_board,
    slug_from_url,
)

BASE = {f: "" for f in FIELDS}


def _with_fetch(payload, fn):
    """Run fn with job_boards.fetch stubbed to return payload."""
    import job_boards
    original = job_boards.fetch
    job_boards.fetch = (
        payload if callable(payload) else (lambda *a, **k: json.dumps(payload).encode())
    )
    try:
        return fn()
    finally:
        job_boards.fetch = original


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def test_slug_parsing():
    assert slug_from_url("https://jobs.ashbyhq.com/0g/4fc6ba8a-1111?ref=x") == "0g"
    assert (
        slug_from_url("https://jobs.ashbyhq.com/A1%20Garage%20Door%20Service/x")
        == "A1 Garage Door Service"
    )
    assert slug_from_url("https://jobs.ashbyhq.com/") is None


def test_cdx_jsonl_parsing():
    """Common Crawl returns JSONL, not a JSON array, and mixes in junk paths."""
    payload = "\n".join([
        '{"url": "https://jobs.ashbyhq.com/ramp/abc-123?ref=x"}',
        '{"url": "https://jobs.ashbyhq.com/Ramp/def-456"}',        # dupe, other casing
        '{"url": "https://jobs.ashbyhq.com/A1%20Garage%20Door/x"}',  # spaces in slug
        "",                                                         # blank line
        '{"url": "https://jobs.ashbyhq.com/_next/static/z.js"}',    # junk, 404s later
        '{"url": "https://jobs.ashbyhq.com/"}',                     # no slug at all
    ])
    seen = {}
    for line in payload.splitlines():
        if not line.strip():
            continue
        slug = slug_from_url(json.loads(line)["url"])
        if slug:
            seen.setdefault(slug.lower(), slug)
    assert sorted(seen.values()) == ["A1 Garage Door", "_next", "ramp"]
    assert seen["ramp"] == "ramp"  # first-seen casing wins over "Ramp"


def test_plausible_rejects_archive_noise_but_keeps_real_slugs():
    """The archives yield millions of URLs; the shape filter makes validation cheap."""
    for good in ("ramp", "keeling-labs", "A1 Garage Door Service", "ScribdInc", "0g"):
        assert plausible(good), good
    for junk in (
        "_next", "api", "favicon.ico", "robots.txt",          # site plumbing
        "root.6511f3ee_758c_4ed4_8fce_254a11715ed7",          # Ashby embed path
        "4fc6ba8a-532f-46a7-b1f3-d5490d78120e",               # a posting id, not a board
        "$10.2K", '"80e0bf43", "environment":"production"',   # comp strings, JS blobs
        "블록웍스", "", "-" * 80,
    ):
        assert not plausible(junk), junk


def test_root_prefix_is_only_junk_for_ashby():
    """`root.<uuid>` is an Ashby embed path; it carries no meaning elsewhere."""
    assert not plausible("root.abc", "ashby")
    assert plausible("root.abc", "greenhouse")


def test_every_posting_api_host_is_pooled():
    """A new ATS added without pooling silently loses the connection reuse.

    Pooling cut connections for a 300-board sample from 300 to 23 and wall clock by
    ~19-29%. That win is per-host, so an adapter whose host is missing from
    _POOLED_HOSTS quietly opts out of it.
    """
    import urllib.parse
    from job_boards import SOURCES, _POOLED_HOSTS, board_url
    for ats in SOURCES:
        if SOURCES[ats].get("api") is None:
            continue
        host = urllib.parse.urlsplit(board_url(ats, "example")).netloc
        assert host in _POOLED_HOSTS, f"{ats} posting API host {host} is not pooled"


def test_etags_are_only_trusted_on_an_unfiltered_run():
    """A 304 only means "no new postings" if the fetch that stored the etag kept
    every posting.

    A --title run persists matching rows only. Trusting its etag later would skip a
    board whose non-matching postings were never recorded, so they would never appear
    even once they matched a later query. Storing and using etags share this gate, so
    an etag in the database always came from a full, persisted fetch.
    """
    from datetime import timedelta
    from job_boards import may_use_etags
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    assert may_use_etags(None, None, None, False, True), "--all --new-only is the one case"
    assert not may_use_etags(None, None, None, False, False), "without --new-only, rows are needed"
    assert not may_use_etags("engineer", None, None, False, True), "--title persists a subset"
    assert not may_use_etags(None, re.compile("x"), None, False, True), "--grep persists a subset"
    assert not may_use_etags(None, None, cutoff, False, True), "--since persists a subset"
    assert not may_use_etags(None, None, None, True, True), "--remote persists a subset"


def test_a_304_skips_the_board():
    """The whole point: an unchanged board costs no body at all."""
    import job_boards as jb
    original = jb._single_request
    jb._single_request = lambda u, m, t, e=None: (304, {"etag": e}, b"")
    try:
        raised = False
        try:
            scan_board("ashby", "acme", None, False, "fuzzy", etag='W/"abc"')
        except jb.NotModified:
            raised = True
        assert raised, "a 304 must surface as NotModified so the caller can skip"
    finally:
        jb._single_request = original


def test_etags_round_trip_through_the_database():
    from job_boards import load_etags, save_etags
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        assert load_etags(db) == {}, "a missing database has no etags"
        save_etags(db, {("ashby", "acme"): 'W/"v1"'}, "2026-01-01T00:00:00+00:00")
        assert load_etags(db) == {("ashby", "acme"): 'W/"v1"'}
        save_etags(db, {("ashby", "acme"): 'W/"v2"'}, "2026-02-02T00:00:00+00:00")
        assert load_etags(db) == {("ashby", "acme"): 'W/"v2"'}, "an etag must be replaced"


def test_conditional_request_sends_if_none_match():
    """Without the header the server has nothing to compare and always returns 200."""
    import job_boards as jb
    seen = {}

    def fake_pooled(url, method, timeout, headers):
        seen.update(headers)
        return 200, {}, b"{}"

    original = jb._pooled_request
    jb._pooled_request = fake_pooled
    try:
        jb._single_request("https://api.lever.co/x", "GET", 5, 'W/"abc"')
        assert seen.get("If-None-Match") == 'W/"abc"'
        seen.clear()
        jb._single_request("https://api.lever.co/x", "GET", 5, None)
        assert "If-None-Match" not in seen, "no etag means no conditional header"
    finally:
        jb._pooled_request = original


def test_header_lookup_is_case_insensitive():
    """urlopen returned a case-insensitive Message; a plain dict is not.

    The pooled path builds its own header dict, so without normalising, a vendor
    sending `content-encoding` instead of `Content-Encoding` would skip gunzipping and
    return compressed bytes as if they were JSON. These three APIs already disagree
    about the casing of `ETag` — ashby and greenhouse send `etag`, lever sends `ETag` —
    so the disagreement is real, not hypothetical.
    """
    import gzip as gziplib
    import job_boards as jb

    payload = gziplib.compress(b'{"jobs": []}')
    for header_name in ("Content-Encoding", "content-encoding", "CONTENT-ENCODING"):
        original = jb._single_request
        jb._single_request = lambda u, m, t, e=None, _h=header_name: (
            200, jb._lower_headers([(_h, "gzip")]), payload
        )
        try:
            assert jb.fetch("https://api.lever.co/x") == b'{"jobs": []}', \
                f"gunzip failed when the server sent {_h!r}"
        finally:
            jb._single_request = original


def test_pooled_request_retries_once_on_a_dead_connection():
    """Servers close idle keep-alive connections; the next use raises, not the close.

    Without the retry a run would surface random failures on boards that happen to
    follow an idle gap.
    """
    import http.client
    import job_boards as jb

    attempts = []

    class FakeConn:
        def __init__(self, host, timeout=None):
            self.host = host
            attempts.append(host)
        def request(self, method, target, headers=None):
            if len(attempts) == 1:      # the first, stale connection fails
                raise http.client.RemoteDisconnected("closed by server")
        def getresponse(self):
            class R:
                status = 200
                def read(self): return b"ok"
                def getheaders(self): return []
            return R()
        def close(self): pass

    original = http.client.HTTPSConnection
    http.client.HTTPSConnection = FakeConn
    jb._CONNECTIONS.__dict__.pop("pool", None)
    try:
        status, _, body = jb._pooled_request("https://api.lever.co/x", "GET", 5, {"User-Agent": "t"})
    finally:
        http.client.HTTPSConnection = original
        jb._CONNECTIONS.__dict__.pop("pool", None)
    assert (status, body) == (200, b"ok")
    assert len(attempts) == 2, "a dead pooled connection must be replaced and retried once"


def test_board_exists_uses_head_and_maps_404_to_false():
    """Validation must not download board payloads — thousands of GETs would be GBs."""
    import job_boards

    calls = []

    def fake_fetch(url, timeout=30, retries=4, method="GET"):
        calls.append(method)
        if "nope" in url:
            raise job_boards.NotFound(url)
        return b""

    original = job_boards.fetch
    job_boards.fetch = fake_fetch
    try:
        assert job_boards.board_exists("ashby", "ramp") is True
        assert job_boards.board_exists("greenhouse", "nope12345") is False
    finally:
        job_boards.fetch = original
    assert calls == ["HEAD", "HEAD"], f"expected HEAD probes, got {calls}"


def test_smartrecruiters_board_exists_rejects_empty_unknown_identifier():
    import job_boards

    original_fetch = job_boards.fetch
    original_career = job_boards._smartrecruiters_career_page_exists
    job_boards.fetch = lambda *args, **kwargs: json.dumps({
        "totalFound": 0, "content": []
    }).encode()
    job_boards._smartrecruiters_career_page_exists = (
        lambda identifier: identifier == "valid-empty-board"
    )
    try:
        assert job_boards.board_exists("smartrecruiters", "valid-empty-board") is True
        assert job_boards.board_exists("smartrecruiters", "unknown-board") is False
    finally:
        job_boards.fetch = original_fetch
        job_boards._smartrecruiters_career_page_exists = original_career


# --------------------------------------------------------------------------- #
# Per-ATS normalisation
# --------------------------------------------------------------------------- #


def test_normalize_ashby():
    row = normalize_ashby({
        "id": "2b401986", "title": "Senior Software Engineer, Data",
        "department": "Builder", "team": "Data", "employmentType": "FullTime",
        "location": "Remote - US", "isRemote": True, "workplaceType": "Remote",
        "address": {
            "postalAddress": {
                "addressCountry": "United States",
                "addressRegion": "CA",
            }
        },
        "secondaryLocations": [],
        "publishedAt": "2026-06-30T19:02:11.162+00:00",
        "jobUrl": "https://jobs.ashbyhq.com/abridge/2b401986",
        "isListed": True, "descriptionPlain": "we use Rust",
    })
    assert row["title"] == "Senior Software Engineer, Data"
    assert row["isRemote"] is True
    assert row["address"]["primary"]["postalAddress"]["addressCountry"] == "United States"
    assert row["address"]["secondaryLocations"] == []
    assert row["publishedAt"].startswith("2026-06-30")
    assert row["_description"] == "we use Rust"
    # unlisted postings are dropped by the normaliser, not downstream
    assert normalize_ashby({"id": "x", "title": "Secret", "isListed": False}) is None


def test_normalize_greenhouse():
    """location is a nested object and there is no remote flag."""
    row = normalize_greenhouse({
        "id": 7954688, "title": "Account Executive",
        "location": {"name": "San Francisco, CA"},
        "absolute_url": "https://stripe.com/jobs/search?gh_jid=7954688",
        "first_published": "2026-06-02T08:58:57-04:00",
        "updated_at": "2026-07-27T11:17:30-04:00",
        "offices": [{"name": "San Francisco", "location": {"name": "California"}}],
        "metadata": [{"name": "team", "value": "Sales"}],
    })
    assert row["id"] == "7954688", "integer ids must become strings for the TEXT key"
    assert row["location"] == "San Francisco, CA"
    assert row["jobUrl"].endswith("gh_jid=7954688")
    assert row["publishedAt"] == "2026-06-02T08:58:57-04:00"
    assert row["isRemote"] is False
    assert row["address"]["offices"][0]["location"]["name"] == "California"
    assert row["address"]["metadata"][0]["name"] == "team"
    assert normalize_greenhouse(
        {"id": 1, "title": "T", "location": {"name": "Remote - US"}}
    )["isRemote"] is True


def test_normalize_lever():
    """Two traps: the title is `text`, and createdAt is epoch milliseconds."""
    row = normalize_lever({
        "id": "f25a6c49", "text": "Compounding Pharmacy Technician",
        "categories": {"commitment": "Full-time", "location": "Romeoville, IL",
                       "team": "Pharmacy", "allLocations": ["Romeoville, IL"]},
        "country": "US",
        "createdAt": 1750119882479,
        "workplaceType": "onsite",
        "hostedUrl": "https://jobs.lever.co/ro/f25a6c49",
        "descriptionPlain": "compounding meds",
    })
    assert row["title"] == "Compounding Pharmacy Technician", "Lever's title key is `text`"
    assert row["employmentType"] == "Full-time"
    assert row["team"] == "Pharmacy"
    assert row["isRemote"] is False
    assert row["address"]["country"] == "US"
    assert row["address"]["allLocations"] == ["Romeoville, IL"]
    # epoch ms -> ISO, or it sorts wrongly against the other platforms
    assert row["publishedAt"].startswith("2025-06-17T"), row["publishedAt"]
    assert "T" in row["publishedAt"] and row["publishedAt"].endswith("+00:00")


def test_normalize_smartrecruiters():
    row = normalize_smartrecruiters({
        "id": 744000148454651,
        "name": "Data Operations Consultant ",
        "releasedDate": "2026-09-09T09:43:26.403Z",
        "location": {
            "city": "Poland",
            "region": "Remote",
            "country": "pl",
            "remote": True,
            "hybrid": False,
            "fullLocation": "Poland, Remote, Poland",
        },
        "department": {"id": "5408931", "label": "Technical Services"},
        "function": {"id": "information_technology", "label": "Information Technology"},
        "typeOfEmployment": {"id": "contract", "label": "Contract"},
        "postingUrl": "https://jobs.smartrecruiters.com/acme/744000148454651-role",
        "jobAd": {
            "sections": {
                "companyDescription": {"text": "<p>Boilerplate</p>"},
                "jobDescription": {"text": "<p>Build APIs</p>"},
                "qualifications": {"text": "<ul><li>Python</li></ul>"},
            }
        },
    })
    assert row["id"] == "744000148454651"
    assert row["location"] == "Poland, Remote, Poland"
    assert row["workplaceType"] == "remote"
    assert row["department"] == "Technical Services"
    assert row["team"] == "Information Technology"
    assert row["employmentType"] == "Contract"
    assert "Build APIs" in row["_description"]
    assert "Boilerplate" not in row["_description"]


def test_normalize_recruitee_preserves_structured_evidence():
    row = normalize_recruitee({
        "id": 2725920,
        "title": "Program Manager",
        "status": "published",
        "department": "Product",
        "employment_type_code": "fulltime_permanent",
        "hybrid": True,
        "locations": [{
            "city": "Nijverdal",
            "state": "Overijssel",
            "country": "Netherlands",
            "country_code": "NL",
        }],
        "published_at": "2026-08-28 11:09:19 UTC",
        "updated_at": "2026-09-10 10:22:54 UTC",
        "careers_url": "https://acme.recruitee.com/o/program-manager",
        "translations": {
            "en": {
                "description": "<p>Lead delivery</p>",
                "requirements": "<ul><li>Stakeholder management</li></ul>",
            }
        },
    })
    assert row["id"] == "2725920"
    assert row["location"] == "Nijverdal, Overijssel, Netherlands"
    assert row["workplaceType"] == "hybrid"
    assert row["publishedAt"] == "2026-08-28T11:09:19+00:00"
    assert row["address"]["locations"][0]["country_code"] == "NL"
    assert "Stakeholder management" in row["_description"]
    assert normalize_recruitee({
        "id": "draft", "title": "Draft", "status": "draft"
    }) is None


def test_normalize_teamtailor_reads_json_feed_jobposting():
    row = normalize_teamtailor({
        "id": "6317ad42-9342-4bb4-8952-d89057311aa9",
        "title": "Executive Assistant",
        "date_published": "2026-09-04T11:33:27+02:00",
        "url": "https://acme.teamtailor.com/jobs/8322261-executive-assistant",
        "content_html": "<p>Coordinate <strong>projects</strong></p>",
        "_jobposting": {
            "datePosted": "2026-09-04T11:33:27+02:00",
            "jobLocation": [{
                "address": {
                    "addressLocality": "Malmo",
                    "addressCountry": "SE",
                }
            }],
            "employmentType": "FULL_TIME",
        },
    })
    assert row["id"].startswith("6317ad42-")
    assert row["location"] == "Malmo, SE"
    assert row["workplaceType"] == "onsite"
    assert row["employmentType"] == "FULL_TIME"
    assert row["publishedAt"] == "2026-09-04T09:33:27+00:00"
    assert row["jobUrl"].endswith("/8322261-executive-assistant")
    assert "Coordinate" in row["_description"]


def test_normalize_workable_handles_public_list_and_detail_fields():
    row = normalize_workable({
        "shortcode": "B19065B177",
        "title": "Senior Account Manager",
        "state": "published",
        "published": "2026-09-04T00:00:00.000Z",
        "remote": False,
        "workplace": "hybrid",
        "department": ["Client Services"],
        "location": {
            "city": "Los Angeles",
            "region": "California",
            "country": "United States",
        },
        "description": "<p>Manage accounts</p>",
        "requirements": "<ul><li>Agency experience</li></ul>",
    })
    assert row["id"] == "B19065B177"
    assert row["department"] == "Client Services"
    assert row["location"] == "Los Angeles, California, United States"
    assert row["workplaceType"] == "hybrid"
    assert row["publishedAt"] == "2026-09-04T00:00:00+00:00"
    assert "Agency experience" in row["_description"]


def test_teamtailor_adapter_follows_json_feed_pagination():
    import job_boards

    original_fetch = job_boards.fetch
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "page=2" in url:
            return json.dumps({
                "version": "https://jsonfeed.org/version/1.1",
                "items": [{"id": "second", "title": "Second"}],
            }).encode()
        return json.dumps({
            "version": "https://jsonfeed.org/version/1.1",
            "items": [{"id": "first", "title": "First"}],
            "next_url": "https://acme.teamtailor.com/jobs.json?page=2&per_page=100",
        }).encode()

    job_boards.fetch = fake_fetch
    try:
        rows = job_boards.fetch_teamtailor_jobs("acme")
    finally:
        job_boards.fetch = original_fetch
    assert len(rows) == 2
    assert any("page=2" in url for url in calls)


def test_workable_adapter_enriches_cutoff_eligible_jobs():
    import job_boards

    original_post = job_boards._workable_post_json
    original_detail = job_boards._workable_detail
    calls = []

    def fake_post(url, payload, timeout=30):
        return {
            "total": 2,
            "results": [
                {
                    "shortcode": "recent",
                    "title": "Recent",
                    "state": "published",
                    "published": "2026-09-01T00:00:00Z",
                },
                {
                    "shortcode": "old",
                    "title": "Old",
                    "state": "published",
                    "published": "2026-07-01T00:00:00Z",
                },
            ],
        }

    def fake_detail(slug, shortcode):
        calls.append((slug, shortcode))
        return {"description": "<p>Recent description</p>"}

    job_boards._workable_post_json = fake_post
    job_boards._workable_detail = fake_detail
    try:
        rows = job_boards.fetch_workable_jobs(
            "acme", datetime(2026, 8, 1, tzinfo=timezone.utc), detail_concurrency=1
        )
    finally:
        job_boards._workable_post_json = original_post
        job_boards._workable_detail = original_detail
    assert [row["shortcode"] for row in rows] == ["recent"]
    assert calls == [("acme", "recent")]


def test_workable_probe_preserves_404_and_429_outcomes():
    import job_boards

    original_pooled = job_boards._pooled_request
    try:
        job_boards._pooled_request = lambda *args, **kwargs: (404, {}, b"")
        invalid = job_boards.probe_workable_board("missing")
        assert invalid["classification"] == "invalid"
        assert invalid["http_status"] == 404
        assert invalid["stop"] is False

        job_boards._pooled_request = lambda *args, **kwargs: (
            429,
            {"retry-after": "120"},
            b'{"error":"rate limited"}',
        )
        throttled = job_boards.probe_workable_board("known-good")
        assert throttled["classification"] == "inconclusive"
        assert throttled["http_status"] == 429
        assert throttled["retry_after_seconds"] == 120.0
        assert throttled["stop"] is True
    finally:
        job_boards._pooled_request = original_pooled


def test_workable_probe_requires_results_array_for_verification():
    import job_boards

    original_pooled = job_boards._pooled_request
    try:
        job_boards._pooled_request = lambda *args, **kwargs: (
            200,
            {},
            b'{"total": 0}',
        )
        result = job_boards.probe_workable_board("incomplete-response")
        assert result["classification"] == "inconclusive"
        assert result["http_status"] == 200

        job_boards._pooled_request = lambda *args, **kwargs: (
            200,
            {},
            b'{"total": 0, "results": []}',
        )
        verified = job_boards.probe_workable_board("empty-but-valid")
        assert verified["classification"] == "verified"
        assert verified["reason"] == "results array (0 jobs)"
    finally:
        job_boards._pooled_request = original_pooled


def test_workable_probe_exports_rate_limit_headers():
    import time
    import job_boards

    original_pooled = job_boards._pooled_request
    try:
        job_boards._pooled_request = lambda *args, **kwargs: (
            200,
            {
                "x-rate-limit-limit": "10",
                "x-rate-limit-remaining": "0",
                "x-rate-limit-reset": str(time.time() + 30),
            },
            b'{"results": []}',
        )
        result = job_boards.probe_workable_board("rate-window")
        assert result["classification"] == "verified"
        assert result["rate_limit_limit"] == 10
        assert result["rate_limit_remaining"] == 0
        assert result["rate_limit_wait_seconds"] > 0
    finally:
        job_boards._pooled_request = original_pooled


def test_smartrecruiters_adapter_paginates_and_enriches_details():
    import job_boards

    original_fetch = job_boards.fetch
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "/postings/101" in url:
            return json.dumps({
                "id": "101",
                "name": "Recent Engineer",
                "releasedDate": "2026-09-15T00:00:00Z",
                "jobAd": {"sections": {"jobDescription": {"text": "<p>Python</p>"}}},
            }).encode()
        if "offset=0" in url:
            return json.dumps({
                "offset": 0, "limit": 100, "totalFound": 101,
                "content": (
                    [{"id": "101", "name": "Recent Engineer"}]
                    + [{"id": str(i), "name": f"Engineer {i}"} for i in range(2, 101)]
                ),
            }).encode()
        return json.dumps({
            "offset": 100, "limit": 100, "totalFound": 101,
            "content": [{"id": "102", "name": "Second Engineer"}],
        }).encode()

    job_boards.fetch = fake_fetch
    try:
        rows = job_boards.fetch_smartrecruiters_jobs(
            "acme", datetime(2026, 9, 14, tzinfo=timezone.utc)
        )
    finally:
        job_boards.fetch = original_fetch
    assert len(rows) == 101
    assert any(url.endswith("offset=0&releasedAfter=2026-09-14T00%3A00%3A00.000%2B00%3A00") for url in calls)
    assert any("/postings/101" in url for url in calls)


def test_normalize_workday():
    row = normalize_workday({
        "title": "Platform Engineer",
        "externalPath": "/job/USA---Remote/Platform-Engineer_JR1",
        "bulletFields": ["JR1"],
        "locationsText": "USA, Remote",
        "postedOn": "Posted Yesterday",
        "remoteType": "Flex",
    })
    assert row["id"] == "JR1"
    assert row["workplaceType"] == "hybrid"
    assert row["isRemote"] is False
    assert row["location"] == "USA, Remote"
    assert row["jobUrl"] == ""
    assert row["publishedAt"].endswith("+00:00")


def test_workday_adapter_paginates_and_namespaces_requisition_ids():
    import job_boards

    original_post = job_boards._workday_post_json
    calls = []

    def fake_post(url, payload, timeout=30):
        calls.append(payload["offset"])
        if payload["offset"] == 0:
            return {
                "total": 21,
                "jobPostings": [
                    {
                        "title": "Platform Engineer",
                        "externalPath": f"/job/Remote/Platform-{i}_JR1",
                        "bulletFields": ["JR1"],
                        "postedOn": "Posted Today",
                    }
                    for i in range(20)
                ],
            }
        return {
            "total": 21,
            "jobPostings": [{
                "title": "Product Engineer",
                "externalPath": "/job/Remote/Product_JR2",
                "bulletFields": ["JR2"],
                "postedOn": "Posted Today",
            }],
        }

    job_boards._workday_post_json = fake_post
    try:
        rows = job_boards.scan_board(
            "workday",
            "tenant.wd5/Careers",
            None,
            False,
            "fuzzy",
        )
    finally:
        job_boards._workday_post_json = original_post
    assert calls == [0, 20]
    assert len(rows) == 21
    assert rows[0]["id"] == "tenant.wd5/Careers:JR1"
    assert rows[-1]["jobUrl"].startswith(
        "https://tenant.wd5.myworkdayjobs.com/en-US/Careers/"
    )


def test_workday_wayback_parser_extracts_locale_and_job_board():
    import job_boards

    original_fetch = job_boards.fetch
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "showNumPages=true" in url:
            return json.dumps([["numpages"], ["1"]]).encode()
        if "wd1.myworkdayjobs.com" not in url:
            return json.dumps([["original"]]).encode()
        return json.dumps([
            ["original"],
            ["https://acme.wd1.myworkdayjobs.com/en-US/External_Careers/job/Remote/Role_JR1"],
            ["https://acme.wd1.myworkdayjobs.com/External_Careers"],
        ]).encode()

    job_boards.fetch = fake_fetch
    try:
        candidates = job_boards.candidates_from_workday_wayback(
            since_days=2, progress_path=None
        )
    finally:
        job_boards.fetch = original_fetch
    assert candidates == {"acme.wd1/external_careers": "acme.wd1/External_Careers"}
    assert len(calls) == 8
    assert all(
        "fl=original" not in url
        for url in calls
        if "showNumPages=true" in url
    )


def test_workday_wayback_parser_reads_multiple_cdx_pages():
    import job_boards

    original_fetch = job_boards.fetch
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "showNumPages=true" in url:
            pages = "2" if "wd1.myworkdayjobs.com" in url else "1"
            return json.dumps([["numpages"], [pages]]).encode()
        if "wd1.myworkdayjobs.com" in url and "page=1" in url:
            return json.dumps([
                ["original"],
                ["https://beta.wd1.myworkdayjobs.com/en-US/Beta_Careers/job/Remote/Role"],
            ]).encode()
        if "wd1.myworkdayjobs.com" in url:
            return json.dumps([
                ["original"],
                ["https://acme.wd1.myworkdayjobs.com/en-US/Acme_Careers/job/Remote/Role"],
            ]).encode()
        return json.dumps([["original"]]).encode()

    job_boards.fetch = fake_fetch
    try:
        candidates = job_boards.candidates_from_workday_wayback(progress_path=None)
    finally:
        job_boards.fetch = original_fetch
    assert candidates == {
        "acme.wd1/acme_careers": "acme.wd1/Acme_Careers",
        "beta.wd1/beta_careers": "beta.wd1/Beta_Careers",
    }
    assert any("page=1" in url for url in calls)


def test_workday_wayback_refuses_malformed_page_count():
    import job_boards

    original_fetch = job_boards.fetch

    def fake_fetch(url, **kwargs):
        return json.dumps([["original"], [None]]).encode()

    job_boards.fetch = fake_fetch
    try:
        try:
            job_boards.candidates_from_workday_wayback(progress_path=None)
        except RuntimeError as exc:
            assert "progress saved for retry" in str(exc)
        else:
            raise AssertionError("malformed CDX page count must fail discovery")
    finally:
        job_boards.fetch = original_fetch


def test_workday_wayback_refuses_a_failed_data_page():
    import job_boards

    original_fetch = job_boards.fetch
    original_delay = job_boards._WORKDAY_CDX_DELAY_SECONDS
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "showNumPages=true" in url:
            return json.dumps([["numpages"], ["2"]]).encode()
        if "page=1" in url:
            raise TimeoutError("timed out")
        return json.dumps([["original"]]).encode()

    job_boards.fetch = fake_fetch
    job_boards._WORKDAY_CDX_DELAY_SECONDS = 0
    try:
        try:
            job_boards.candidates_from_workday_wayback(progress_path=None)
        except RuntimeError as exc:
            assert "unresolved pages" in str(exc)
            assert "progress saved" in str(exc)
        else:
            raise AssertionError("a failed CDX data page must fail discovery")
        assert any("wd3.myworkdayjobs.com" in url for url in calls)
    finally:
        job_boards.fetch = original_fetch
        job_boards._WORKDAY_CDX_DELAY_SECONDS = original_delay


def test_workday_wayback_stops_after_three_consecutive_page_failures():
    import job_boards

    original_fetch = job_boards.fetch
    original_delay = job_boards._WORKDAY_CDX_DELAY_SECONDS
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "showNumPages=true" in url:
            return json.dumps([["numpages"], ["5"]]).encode()
        if any(f"page={page}" in url for page in (1, 2, 3)):
            raise TimeoutError("timed out")
        return json.dumps([["original"]]).encode()

    job_boards.fetch = fake_fetch
    job_boards._WORKDAY_CDX_DELAY_SECONDS = 0
    try:
        try:
            job_boards.candidates_from_workday_wayback(progress_path=None)
        except RuntimeError as exc:
            assert "3 consecutive CDX page failures" in str(exc)
        else:
            raise AssertionError("three consecutive failures must stop discovery")
        assert not any("page=4" in url for url in calls)
    finally:
        job_boards.fetch = original_fetch
        job_boards._WORKDAY_CDX_DELAY_SECONDS = original_delay


def test_workday_wayback_resumes_from_saved_page():
    import job_boards

    original_fetch = job_boards.fetch
    original_delay = job_boards._WORKDAY_CDX_DELAY_SECONDS
    calls = []
    fail_page_once = True

    def fake_fetch(url, **kwargs):
        nonlocal fail_page_once
        calls.append(url)
        if "showNumPages=true" in url:
            pages = "3" if "wd1.myworkdayjobs.com" in url else "1"
            return json.dumps([["numpages"], [pages]]).encode()
        if "wd1.myworkdayjobs.com" in url and "page=1" in url and fail_page_once:
            fail_page_once = False
            raise TimeoutError("timed out")
        tenant = "alpha" if "page=" not in url else (
            "beta" if "page=1" in url else "charlie"
        )
        return json.dumps([
            ["original"],
            [f"https://{tenant}.wd1.myworkdayjobs.com/en-US/Careers"],
        ]).encode()

    job_boards.fetch = fake_fetch
    job_boards._WORKDAY_CDX_DELAY_SECONDS = 0
    try:
        with tempfile.TemporaryDirectory() as tmp:
            progress_path = Path(tmp) / "workday-progress.json"
            try:
                job_boards.candidates_from_workday_wayback(
                    progress_path=progress_path
                )
            except RuntimeError:
                pass
            else:
                raise AssertionError("first run should stop on the simulated timeout")
            assert progress_path.exists()
            first_page_calls = sum(
                "wd1.myworkdayjobs.com" in url
                and "showNumPages=true" not in url
                and "page=" not in url
                for url in calls
            )
            candidates = job_boards.candidates_from_workday_wayback(
                progress_path=progress_path
            )
            assert first_page_calls == 1
            assert sum(
                "wd1.myworkdayjobs.com" in url
                and "showNumPages=true" not in url
                and "page=" not in url
                for url in calls
            ) == 1
            assert progress_path.exists()
            assert json.loads(progress_path.read_text())["crawlComplete"] is True
            assert {
                "alpha.wd1/careers",
                "beta.wd1/careers",
                "charlie.wd1/careers",
            }.issubset(candidates)
    finally:
        job_boards.fetch = original_fetch
        job_boards._WORKDAY_CDX_DELAY_SECONDS = original_delay


def test_workday_validation_saves_live_boards_and_preserves_errors_for_retry():
    import job_boards

    original_candidates = job_boards.candidates_from_workday_wayback
    original_inspect = job_boards.inspect_workday_board
    original_seed = job_boards.BOARDS_SEED
    original_cache = job_boards.BOARDS_CACHE
    original_report = job_boards.WORKDAY_DISCOVERY_REPORT
    original_progress = job_boards.WORKDAY_DISCOVERY_PROGRESS
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job_boards.BOARDS_SEED = root / "boards.seed.json"
            job_boards.BOARDS_CACHE = root / "boards.json"
            job_boards.WORKDAY_DISCOVERY_REPORT = root / "report.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS = root / "progress.json"
            job_boards.BOARDS_SEED.write_text('{"workday": []}')
            job_boards.BOARDS_CACHE.write_text(
                '{"workday": ["existing.wd1/Careers"]}'
            )
            job_boards.WORKDAY_DISCOVERY_PROGRESS.write_text('{"crawlComplete": true}')
            job_boards.candidates_from_workday_wayback = lambda *args, **kwargs: {
                "live.wd1/careers": "live.wd1/Careers",
                "error.wd1/careers": "error.wd1/Careers",
            }
            job_boards.inspect_workday_board = lambda identifier, recent_days=None: {
                "identifier": identifier,
                "live": identifier.startswith("live."),
                "outcome": (
                    "live" if identifier.startswith("live.") else "request_error"
                ),
                "error": None if identifier.startswith("live.") else "timed out",
            }
            try:
                job_boards.discover_workday_boards(concurrency=1)
            except RuntimeError as exc:
                assert "validation had 1 request errors" in str(exc)
                assert "1 were added to boards.json" in str(exc)
                assert "only the request errors need retrying" in str(exc)
            else:
                raise AssertionError("request errors must block cache replacement")
            assert json.loads(job_boards.BOARDS_CACHE.read_text()) == {
                "workday": ["existing.wd1/Careers", "live.wd1/Careers"]
            }
            assert job_boards.WORKDAY_DISCOVERY_PROGRESS.exists()
            assert json.loads(job_boards.WORKDAY_DISCOVERY_REPORT.read_text())[
                "requestErrorCount"
            ] == 1
    finally:
        job_boards.candidates_from_workday_wayback = original_candidates
        job_boards.inspect_workday_board = original_inspect
        job_boards.BOARDS_SEED = original_seed
        job_boards.BOARDS_CACHE = original_cache
        job_boards.WORKDAY_DISCOVERY_REPORT = original_report
        job_boards.WORKDAY_DISCOVERY_PROGRESS = original_progress


def test_workday_validation_resume_only_retries_request_errors():
    import job_boards

    original_candidates = job_boards.candidates_from_workday_wayback
    original_inspect = job_boards.inspect_workday_board
    original_seed = job_boards.BOARDS_SEED
    original_cache = job_boards.BOARDS_CACHE
    original_report = job_boards.WORKDAY_DISCOVERY_REPORT
    original_progress = job_boards.WORKDAY_DISCOVERY_PROGRESS
    calls = []
    flaky_attempts = 0
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job_boards.BOARDS_SEED = root / "boards.seed.json"
            job_boards.BOARDS_CACHE = root / "boards.json"
            job_boards.WORKDAY_DISCOVERY_REPORT = root / "report.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS = root / "progress.json"
            job_boards.BOARDS_SEED.write_text('{"workday": []}')
            job_boards.BOARDS_CACHE.write_text('{"workday": []}')
            job_boards.WORKDAY_DISCOVERY_PROGRESS.write_text(
                json.dumps({
                    "crawlComplete": True,
                    "validations": {
                        "missing.wd1/careers": {
                            "identifier": "missing.wd1/Careers",
                            "live": False,
                            "outcome": "request_error",
                            "errorType": "NotFound",
                            "error": "missing URL",
                            "attempts": 1,
                        },
                    },
                })
            )
            job_boards.candidates_from_workday_wayback = lambda *args, **kwargs: {
                "live.wd1/careers": "live.wd1/Careers",
                "invalid.wd1/careers": "invalid.wd1/Careers",
                "flaky.wd1/careers": "flaky.wd1/Careers",
                "missing.wd1/careers": "missing.wd1/Careers",
            }

            def inspect(identifier, recent_days=None):
                nonlocal flaky_attempts
                calls.append(identifier)
                if identifier.startswith("invalid."):
                    return {
                        "identifier": identifier,
                        "live": False,
                        "outcome": "invalid_response",
                    }
                if identifier.startswith("flaky.") and flaky_attempts == 0:
                    flaky_attempts += 1
                    return {
                        "identifier": identifier,
                        "live": False,
                        "outcome": "request_error",
                        "error": "timed out",
                    }
                return {
                    "identifier": identifier,
                    "live": True,
                    "outcome": "live",
                    "hasRecentJob": False,
                }

            job_boards.inspect_workday_board = inspect
            try:
                job_boards.discover_workday_boards(concurrency=1)
            except RuntimeError:
                pass
            else:
                raise AssertionError("the first flaky validation must remain retryable")
            calls.clear()
            result = job_boards.discover_workday_boards(concurrency=1)
            assert calls == ["flaky.wd1/Careers"]
            assert result == ["flaky.wd1/Careers", "live.wd1/Careers"]
            validations = json.loads(
                job_boards.WORKDAY_DISCOVERY_PROGRESS.read_text()
            )["validations"]
            assert validations["live.wd1/careers"]["attempts"] == 1
            assert validations["invalid.wd1/careers"]["attempts"] == 1
            assert validations["flaky.wd1/careers"]["attempts"] == 2
            assert validations["missing.wd1/careers"]["outcome"] == "invalid_response"
            assert validations["missing.wd1/careers"]["attempts"] == 1
    finally:
        job_boards.candidates_from_workday_wayback = original_candidates
        job_boards.inspect_workday_board = original_inspect
        job_boards.BOARDS_SEED = original_seed
        job_boards.BOARDS_CACHE = original_cache
        job_boards.WORKDAY_DISCOVERY_REPORT = original_report
        job_boards.WORKDAY_DISCOVERY_PROGRESS = original_progress


def test_validate_discovered_uses_checkpoint_without_wayback_and_keeps_it():
    import job_boards

    original_candidates = job_boards.candidates_from_workday_wayback
    original_inspect = job_boards.inspect_workday_board
    original_seed = job_boards.BOARDS_SEED
    original_cache = job_boards.BOARDS_CACHE
    original_report = job_boards.WORKDAY_DISCOVERY_REPORT
    original_progress = job_boards.WORKDAY_DISCOVERY_PROGRESS
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job_boards.BOARDS_SEED = root / "boards.seed.json"
            job_boards.BOARDS_CACHE = root / "boards.json"
            job_boards.WORKDAY_DISCOVERY_REPORT = root / "report.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS = root / "progress.json"
            job_boards.BOARDS_SEED.write_text('{"workday": []}')
            job_boards.BOARDS_CACHE.write_text('{"workday": []}')
            job_boards.WORKDAY_DISCOVERY_PROGRESS.write_text(json.dumps({
                "crawlComplete": False,
                "seen": {"live.wd1/careers": "live.wd1/Careers"},
            }))
            job_boards.candidates_from_workday_wayback = lambda *args, **kwargs: (
                (_ for _ in ()).throw(AssertionError("Wayback must not be contacted"))
            )
            job_boards.inspect_workday_board = lambda identifier, recent_days=None: {
                "identifier": identifier,
                "live": True,
                "outcome": "live",
                "hasRecentJob": False,
            }

            boards = job_boards.load_boards(
                True,
                ["workday"],
                concurrency=1,
                validate_discovered=True,
            )

            assert boards == {"workday": ["live.wd1/Careers"]}
            assert job_boards.WORKDAY_DISCOVERY_PROGRESS.exists()
            progress = json.loads(job_boards.WORKDAY_DISCOVERY_PROGRESS.read_text())
            assert progress["crawlComplete"] is False
            assert progress["validations"]["live.wd1/careers"]["outcome"] == "live"
    finally:
        job_boards.candidates_from_workday_wayback = original_candidates
        job_boards.inspect_workday_board = original_inspect
        job_boards.BOARDS_SEED = original_seed
        job_boards.BOARDS_CACHE = original_cache
        job_boards.WORKDAY_DISCOVERY_REPORT = original_report
        job_boards.WORKDAY_DISCOVERY_PROGRESS = original_progress


def test_workday_validation_classifies_definitive_http_failures_as_invalid():
    import job_boards

    original_post = job_boards._workday_post_json
    try:
        for error in (
            job_boards.NotFound("missing"),
            job_boards.urllib.error.HTTPError(
                "https://example.test", 422, "client error", {}, None
            ),
        ):
            job_boards._workday_post_json = lambda *args, error=error, **kwargs: (
                (_ for _ in ()).throw(error)
            )
            result = job_boards.inspect_workday_board("tenant.wd1/Careers")
            assert result["outcome"] == "invalid_response"

        forbidden = job_boards.urllib.error.HTTPError(
            "https://example.test", 403, "client error", {}, None
        )
        job_boards._workday_post_json = lambda *args, **kwargs: (
            (_ for _ in ()).throw(forbidden)
        )
        result = job_boards.inspect_workday_board("tenant.wd1/Careers")
        assert result["outcome"] == "request_error"
    finally:
        job_boards._workday_post_json = original_post


def test_successful_workday_cache_write_removes_checkpoint():
    import job_boards

    original_discover = job_boards.discover_boards
    original_cache = job_boards.BOARDS_CACHE
    original_progress = job_boards.WORKDAY_DISCOVERY_PROGRESS
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job_boards.BOARDS_CACHE = root / "boards.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS = root / "progress.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS.write_text('{"crawlComplete": true}')
            job_boards.discover_boards = lambda *args, **kwargs: [
                "new.wd1/Careers"
            ]
            boards = job_boards.load_boards(True, ["workday"])
            assert boards == {"workday": ["new.wd1/Careers"]}
            assert not job_boards.WORKDAY_DISCOVERY_PROGRESS.exists()
    finally:
        job_boards.discover_boards = original_discover
        job_boards.BOARDS_CACHE = original_cache
        job_boards.WORKDAY_DISCOVERY_PROGRESS = original_progress


def test_failed_final_workday_cache_commit_preserves_cache_and_checkpoint():
    import job_boards

    original_discover = job_boards.discover_boards
    original_write = job_boards._write_json_atomic
    original_cache = job_boards.BOARDS_CACHE
    original_progress = job_boards.WORKDAY_DISCOVERY_PROGRESS
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job_boards.BOARDS_CACHE = root / "boards.json"
            job_boards.WORKDAY_DISCOVERY_PROGRESS = root / "progress.json"
            original_payload = {"workday": ["existing.wd1/Careers"]}
            job_boards.BOARDS_CACHE.write_text(json.dumps(original_payload))
            job_boards.WORKDAY_DISCOVERY_PROGRESS.write_text(
                '{"crawlComplete": true}'
            )
            job_boards.discover_boards = lambda *args, **kwargs: [
                "new.wd1/Careers"
            ]

            def fail_write(path, payload):
                raise OSError("disk full")

            job_boards._write_json_atomic = fail_write
            try:
                job_boards.load_boards(True, ["workday"])
            except OSError as exc:
                assert "disk full" in str(exc)
            else:
                raise AssertionError("the simulated final cache write must fail")
            assert json.loads(job_boards.BOARDS_CACHE.read_text()) == original_payload
            assert job_boards.WORKDAY_DISCOVERY_PROGRESS.exists()
    finally:
        job_boards.discover_boards = original_discover
        job_boards._write_json_atomic = original_write
        job_boards.BOARDS_CACHE = original_cache
        job_boards.WORKDAY_DISCOVERY_PROGRESS = original_progress


def test_workday_cutoff_enrichment_skips_old_list_items():
    import job_boards
    from datetime import timedelta

    original_post = job_boards._workday_post_json
    original_details = job_boards._workday_details
    detail_calls = []

    def fake_post(url, payload, timeout=30):
        return {
            "total": 2,
            "jobPostings": [
                {
                    "title": "Recent role",
                    "externalPath": "/job/Remote/Recent_JR1",
                    "bulletFields": ["JR1"],
                    "postedOn": "Posted Today",
                },
                {
                    "title": "Old role",
                    "externalPath": "/job/Remote/Old_JR2",
                    "bulletFields": ["JR2"],
                    "postedOn": "Posted 30+ Days Ago",
                },
            ],
        }

    def fake_details(url):
        detail_calls.append(url)
        return {"description": "Recent detail"}

    job_boards._workday_post_json = fake_post
    job_boards._workday_details = fake_details
    try:
        rows = job_boards.fetch_workday_jobs(
            "tenant.wd5/Careers",
            datetime.now(timezone.utc) - timedelta(days=2),
        )
    finally:
        job_boards._workday_post_json = original_post
        job_boards._workday_details = original_details
    assert len(rows) == 2
    assert len(detail_calls) == 1
    assert "Recent_JR1" in detail_calls[0]


def test_workday_cutoff_stops_after_detail_dates_cross_cutoff():
    import job_boards
    from datetime import timedelta

    original_post = job_boards._workday_post_json
    original_details = job_boards._workday_details
    offsets = []

    def fake_post(url, payload, timeout=30):
        offset = payload["offset"]
        offsets.append(offset)
        age = "Recent" if offset == 0 else "Old"
        return {
            "total": 60,
            "jobPostings": [
                {
                    "title": f"{age} role {index}",
                    "externalPath": f"/job/Remote/{age}_{offset}_{index}",
                    "bulletFields": [f"{age}-{offset}-{index}"],
                }
                for index in range(20)
            ],
        }

    def fake_details(url):
        return {
            "datePosted": (
                datetime.now(timezone.utc).date().isoformat()
                if "/Recent_" in url
                else "2020-01-01"
            )
        }

    job_boards._workday_post_json = fake_post
    job_boards._workday_details = fake_details
    try:
        rows = job_boards.fetch_workday_jobs(
            "tenant.wd5/Careers",
            datetime.now(timezone.utc) - timedelta(days=2),
        )
    finally:
        job_boards._workday_post_json = original_post
        job_boards._workday_details = original_details
    assert offsets == [0, 20]
    assert len(rows) == 40


def test_workday_reuses_cached_detail_evidence():
    import job_boards

    original_post = job_boards._workday_post_json
    original_details = job_boards._workday_details

    def fake_post(url, payload, timeout=30):
        return {
            "total": 1,
            "jobPostings": [{
                "title": "Cached role",
                "externalPath": "/job/Remote/Cached_JR1",
                "bulletFields": ["JR1"],
                "postedOn": "Posted Today",
            }],
        }

    def should_not_fetch_details(url):
        raise AssertionError("cached Workday details should avoid this request")

    job_boards._workday_post_json = fake_post
    job_boards._workday_details = should_not_fetch_details
    try:
        rows = job_boards.fetch_workday_jobs(
            "tenant.wd5/Careers",
            datetime.now(timezone.utc),
            {
                "JR1": {
                    "published_at": "2026-09-12T00:00:00+00:00",
                    "description_text": "Cached description",
                    "location_raw": "Sofia",
                    "workplace_type": "hybrid",
                    "employment_type": "Full time",
                }
            },
        )
    finally:
        job_boards._workday_post_json = original_post
        job_boards._workday_details = original_details
    assert rows[0]["jobDescription"] == "Cached description"
    assert rows[0]["locationsText"] == "Sofia"
    assert rows[0]["remoteType"] == "hybrid"


def test_workday_inspection_reports_recent_jobs():
    import job_boards

    original_post = job_boards._workday_post_json

    def fake_post(url, payload, timeout=30):
        return {
            "total": 12,
            "jobPostings": [{
                "title": "Recent role",
                "externalPath": "/job/Remote/Recent_JR1",
                "postedOn": "Posted Today",
            }],
        }

    job_boards._workday_post_json = fake_post
    try:
        status = job_boards.inspect_workday_board(
            "tenant.wd5/Careers",
            recent_days=30,
        )
    finally:
        job_boards._workday_post_json = original_post
    assert status["live"] is True
    assert status["outcome"] == "live"
    assert status["totalJobs"] == 12
    assert status["hasRecentJob"] is True
    assert status["newestPostedAt"]


def test_workday_inspection_accounts_for_invalid_responses():
    import job_boards

    original_post = job_boards._workday_post_json
    job_boards._workday_post_json = lambda *args, **kwargs: {"total": 1}
    try:
        status = job_boards.inspect_workday_board("tenant.wd5/Careers")
    finally:
        job_boards._workday_post_json = original_post
    assert status["live"] is False
    assert status["outcome"] == "invalid_response"
    assert "jobPostings" in status["error"]


def test_workday_inspection_accounts_for_request_errors():
    import job_boards

    original_post = job_boards._workday_post_json

    def fail(*args, **kwargs):
        raise TimeoutError("timed out")

    job_boards._workday_post_json = fail
    try:
        status = job_boards.inspect_workday_board("tenant.wd5/Careers")
    finally:
        job_boards._workday_post_json = original_post
    assert status["live"] is False
    assert status["outcome"] == "request_error"
    assert status["errorType"] == "TimeoutError"


def test_normalizers_survive_explicit_nulls():
    """Every ATS sends JSON null for fields it has no value for.

    Regression: Greenhouse sends {"location": {"name": null}}. `loc.get("name", "")`
    returns None there — the key exists, so the default never applies — and the
    subsequent .lower() raised, aborting the whole board. Seven Greenhouse boards
    were dropped entirely, xai's 220 jobs among them.
    """
    gh = normalize_greenhouse({"id": 1, "title": None, "location": {"name": None},
                               "absolute_url": None, "first_published": None})
    assert gh["location"] == "" and gh["isRemote"] is False and gh["title"] == ""

    ash = normalize_ashby({"id": "1", "isListed": True, "title": None,
                           "location": None, "descriptionPlain": None})
    assert ash["title"] == "" and ash["location"] == ""

    lev = normalize_lever({"id": "1", "text": None, "categories": None,
                           "createdAt": None, "workplaceType": None})
    assert lev["title"] == "" and lev["publishedAt"] == "" and lev["team"] == ""


def test_every_normalizer_fills_the_same_keys():
    """A missing key in one adapter becomes a silently empty column."""
    samples = [
        (normalize_ashby, {"id": "1", "isListed": True}),
        (normalize_greenhouse, {"id": 1}),
        (normalize_lever, {"id": "1"}),
        (normalize_workday, {
            "title": "T", "externalPath": "/job/Location/T_JR1",
        }),
    ]
    expected = {f for f in FIELDS if f not in ("ats", "company", "matched")}
    for fn, payload in samples:
        keys = set(fn(payload)) - {"_description"}
        assert keys == expected, f"{fn.__name__} produced {keys ^ expected}"


def test_greenhouse_content_param_only_when_grepping():
    """Descriptions cost ~26x the bytes, so they must be opt-in."""
    assert "content=true" not in board_url("greenhouse", "stripe")
    assert "content=true" in board_url("greenhouse", "stripe", want_content=True)
    # platforms that always return descriptions must not gain a stray parameter
    assert "content=true" not in board_url("ashby", "ramp", want_content=True)
    assert "mode=json" in board_url("lever", "ro", want_content=True)


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #


def test_fuzzy_matching():
    assert matches("Senior Software Engineer, Backend", "software engineer")
    assert matches("SOFTWARE ENGINEER II", "software engineer")
    assert matches("Software Engineer", "senior software engineer, backend")
    assert not matches("Engineering Manager", "software engineer")


def test_fuzzy_does_not_match_generic_one_word_titles():
    """The reverse direction must not drag in every one-word title in the query."""
    for junk in ("Engineer", "Software", "Senior"):
        assert not matches(junk, "senior software engineer"), junk
    assert matches("Software Engineer", "senior software engineer")


def test_empty_query_matches_nothing():
    assert not matches("Chef", "")
    assert not matches("", "software engineer")


def test_exact_matching():
    assert matches("Software Engineer", "software engineer", "exact")
    assert matches("  SOFTWARE ENGINEER  ", "software engineer", "exact")
    assert not matches("Senior Software Engineer, Backend", "software engineer", "exact")
    assert not matches("Software Engineer", "senior software engineer", "exact")


def test_plain_text_strips_markup_and_entities():
    assert plain_text("<p>Rust &amp; Go</p>\n\n  <b>here</b>") == "Rust & Go here"
    assert plain_text("") == ""


def test_fragments_give_context_and_dedupe():
    text = "we use kubernetes daily. " * 3 + "the end"
    hits = fragments(text, re.compile("kubernetes", re.IGNORECASE))
    assert len(hits) <= 2
    assert all("kubernetes" in h for h in hits)
    assert len(set(hits)) == len(hits)
    assert fragments("nothing here", re.compile("kubernetes")) == []


def test_scan_board_filters_and_flattens():
    """isListed/remote filtering, and that descriptions never reach the output."""
    board = {"jobs": [
        {"id": "1", "title": "Software Engineer", "isListed": True, "isRemote": True,
         "location": "Remote", "descriptionHtml": "<p>huge</p>",
         "descriptionPlain": "huge", "jobUrl": "u1"},
        {"id": "2", "title": "Software Engineer", "isListed": False, "isRemote": True},
        {"id": "3", "title": "Chef", "isListed": True, "isRemote": True},
        {"id": "4", "title": "Software Engineer II", "isListed": True, "isRemote": False},
    ]}
    rows = _with_fetch(board, lambda: scan_board("ashby", "acme", "software engineer", False, "fuzzy"))
    remote = _with_fetch(board, lambda: scan_board("ashby", "acme", "software engineer", True, "fuzzy"))

    assert [r["title"] for r in rows] == ["Software Engineer", "Software Engineer II"]
    assert [r["title"] for r in remote] == ["Software Engineer"]
    assert rows[0]["company"] == "acme" and rows[0]["ats"] == "ashby"
    assert set(rows[0]) == set(FIELDS), "row must be exactly the declared columns"
    assert not any("escription" in k for k in rows[0]), "descriptions must be dropped"


def test_scan_board_handles_levers_bare_list_payload():
    """Lever's payload is the list itself, not {'jobs': [...]}."""
    payload = [{"id": "a", "text": "Software Engineer", "categories": {},
                "createdAt": 1750119882479, "hostedUrl": "u"}]
    rows = _with_fetch(payload, lambda: scan_board("lever", "ro", "software engineer", False, "fuzzy"))
    assert len(rows) == 1
    assert rows[0]["ats"] == "lever" and rows[0]["title"] == "Software Engineer"


def test_grep_filters_on_description_and_records_context():
    board = {"jobs": [
        {"id": "1", "title": "Backend Engineer", "isListed": True,
         "descriptionPlain": "You will write <b>Rust</b> and Go all day."},
        {"id": "2", "title": "Backend Engineer", "isListed": True,
         "descriptionPlain": "We deeply value trust and integrity."},
    ]}
    loose = _with_fetch(board, lambda: scan_board(
        "ashby", "acme", None, False, "fuzzy", re.compile("rust", re.I)))
    strict = _with_fetch(board, lambda: scan_board(
        "ashby", "acme", None, False, "fuzzy", re.compile(r"\brust\b", re.I)))

    assert len(loose) == 2, "unbounded 'rust' also matches 'trust' — the documented footgun"
    assert len(strict) == 1, "word-bounded regex excludes 'trust'"
    assert "Rust" in strict[0]["matched"]
    assert "<b>" not in strict[0]["matched"], "markup must be stripped from fragments"


def test_grep_matches_the_title_not_only_the_description():
    """A posting whose subject is in its title was silently dropped.

    Searching the description alone missed "SSO Integrations Lead" whenever the
    body phrased it differently — exactly the postings a topic search most wants.
    """
    board = {"jobs": [
        {"id": "1", "title": "SSO Integrations Lead", "isListed": True,
         "descriptionPlain": "Own the identity roadmap end to end."},
        {"id": "2", "title": "Warehouse Associate", "isListed": True,
         "descriptionPlain": "Lift boxes."},
    ]}
    rows = _with_fetch(board, lambda: scan_board(
        "ashby", "acme", None, False, "fuzzy", re.compile(r"\bsso\b", re.I)))
    assert [r["title"] for r in rows] == ["SSO Integrations Lead"]
    assert "SSO" in rows[0]["matched"], "the title hit must land in the matched column"


def test_grep_does_not_match_across_the_title_description_seam():
    """Title and description are searched apart, not concatenated.

    Concatenating would let `engineer we` match the join between a title ending
    "Engineer" and a body starting "We", reporting a hit present in neither field.
    """
    board = {"jobs": [
        {"id": "1", "title": "Backend Engineer", "isListed": True,
         "descriptionPlain": "We ship every day."},
    ]}
    rows = _with_fetch(board, lambda: scan_board(
        "ashby", "acme", None, False, "fuzzy", re.compile(r"engineer we", re.I)))
    assert rows == [], "a match spanning the seam belongs to neither field"


def test_invalid_payload_raises_rather_than_returning_nothing():
    raised = False
    try:
        _with_fetch({"error": "nope"}, lambda: scan_board("ashby", "acme", "engineer", False, "fuzzy"))
    except ValueError:
        raised = True
    assert raised, "a shape change must fail loudly, not read as 'no jobs found'"


def test_no_filters_returns_every_listed_job():
    board = {"jobs": [
        {"id": "1", "title": "Chef", "isListed": True},
        {"id": "2", "title": "Welder", "isListed": True},
        {"id": "3", "title": "Secret Role", "isListed": False},
    ]}
    rows = _with_fetch(board, lambda: scan_board("ashby", "acme", None, False, "fuzzy", None))
    assert [r["title"] for r in rows] == ["Chef", "Welder"]


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #


def test_db_upsert_preserves_history():
    """first_seen must survive re-scrapes; that is the point of the database."""
    row = BASE | {"ats": "ashby", "id": "job-1", "company": "acme", "title": "SWE"}
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        new, updated, _ = save([row], db, "2026-01-01T00:00:00+00:00")
        assert (new, updated) == (1, 0)
        new, updated, _ = save([row | {"title": "Senior SWE"}], db, "2026-02-02T00:00:00+00:00")
        assert (new, updated) == (0, 1)
        got = sqlite3.connect(db).execute(
            "SELECT title, first_seen, last_seen FROM jobs").fetchall()
        assert got == [("Senior SWE", "2026-01-01T00:00:00+00:00", "2026-02-02T00:00:00+00:00")]


def test_same_id_on_two_platforms_is_two_rows():
    """Greenhouse ids are integers; a bare id key would collide across platforms."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        save([
            BASE | {"ats": "ashby", "id": "123", "company": "acme", "title": "A"},
            BASE | {"ats": "greenhouse", "id": "123", "company": "other", "title": "G"},
        ], db, "2026-01-01T00:00:00+00:00")
        got = dict(sqlite3.connect(db).execute("SELECT ats, title FROM jobs"))
        assert got == {"ashby": "A", "greenhouse": "G"}


def test_db_keeps_grep_context_from_earlier_runs():
    row = BASE | {"ats": "ashby", "id": "job-1", "company": "acme"}
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        save([row | {"matched": "uses Rust daily"}], db, "2026-01-01T00:00:00+00:00")
        save([row], db, "2026-02-02T00:00:00+00:00")  # no --grep this time
        (kept,) = sqlite3.connect(db).execute("SELECT matched FROM jobs").fetchone()
        assert kept == "uses Rust daily"


def test_migrates_a_single_ats_database():
    """Upgrade path: an id-keyed, ats-less database from before Greenhouse existed.

    Fresh-schema tests never touch this branch, which is exactly how the previous
    migration shipped broken.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "old.db"
        legacy = [f for f in FIELDS if f not in ("ats", "id")]
        con = sqlite3.connect(db)
        con.executescript(f"""
            CREATE TABLE jobs (id TEXT PRIMARY KEY,
                               {','.join(f'{c} TEXT' for c in legacy)},
                               first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
                               closed_at TEXT);
            CREATE INDEX jobs_company ON jobs(company);
        """)
        con.execute(
            "INSERT INTO jobs (id, company, title, first_seen, last_seen) VALUES "
            "('old-1','acme','Old Role','2026-01-01T00:00:00+00:00',"
            "'2026-01-01T00:00:00+00:00')"
        )
        con.commit()
        con.close()

        row = BASE | {"ats": "greenhouse", "id": "new-1", "company": "gh-co"}
        save([row], db, "2026-02-02T00:00:00+00:00")

        con = sqlite3.connect(db)
        cols = {c[1] for c in con.execute("PRAGMA table_info(jobs)")}
        assert "ats" in cols and "closed_at" in cols
        got = dict(con.execute("SELECT id, ats FROM jobs"))
        assert got == {"old-1": "ashby", "new-1": "greenhouse"}, \
            "existing rows must be labelled ashby, not dropped"
        # history survives the table rebuild
        (fs, title) = con.execute(
            "SELECT first_seen, title FROM jobs WHERE id='old-1'").fetchone()
        assert fs == "2026-01-01T00:00:00+00:00" and title == "Old Role"
        assert (("ats", "id") == tuple(
            c[1] for c in con.execute("PRAGMA table_info(jobs)") if c[5]
        )), "primary key must be (ats, id)"


def test_migrates_a_database_created_before_closed_at():
    """The older upgrade path still works, now via the rebuild."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "old.db"
        legacy = [f for f in FIELDS if f not in ("ats", "id")]
        con = sqlite3.connect(db)
        con.executescript(f"""
            CREATE TABLE jobs (id TEXT PRIMARY KEY,
                               {','.join(f'{c} TEXT' for c in legacy)},
                               first_seen TEXT NOT NULL, last_seen TEXT NOT NULL);
        """)
        con.execute(
            "INSERT INTO jobs (id, company, first_seen, last_seen) VALUES "
            "('old-1','acme','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')"
        )
        con.commit()
        con.close()

        row = BASE | {"ats": "ashby", "id": "new-1", "company": "acme"}
        new, _, closed = save([row], db, "2026-02-02T00:00:00+00:00",
                              covered=[("ashby", "acme")])
        con = sqlite3.connect(db)
        assert "closed_at" in {c[1] for c in con.execute("PRAGMA table_info(jobs)")}
        got = dict(con.execute("SELECT id, closed_at FROM jobs"))
        assert got["old-1"] == "2026-02-02T00:00:00+00:00"
        assert got["new-1"] is None
        assert (new, closed) == (1, 1)
        idx = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert "jobs_closed_at" in idx


def test_unfiltered_run_closes_vanished_postings():
    a = BASE | {"ats": "ashby", "id": "a", "company": "acme"}
    b = BASE | {"ats": "ashby", "id": "b", "company": "acme"}
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        save([a, b], db, "2026-01-01T00:00:00+00:00", covered=[("ashby", "acme")])
        _, _, closed = save([a], db, "2026-02-02T00:00:00+00:00",
                            covered=[("ashby", "acme")])
        assert closed == 1
        got = dict(sqlite3.connect(db).execute("SELECT id, closed_at FROM jobs"))
        assert got["a"] is None and got["b"] == "2026-02-02T00:00:00+00:00"

        _, _, closed = save([a, b], db, "2026-03-03T00:00:00+00:00",
                            covered=[("ashby", "acme")])
        assert closed == 0
        got = dict(sqlite3.connect(db).execute("SELECT id, closed_at FROM jobs"))
        assert got["b"] is None, "a reappearing posting must reopen"


def test_filtered_run_never_closes_anything():
    """A --title run missing a job is ambiguous: gone, or just not matched."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        save([BASE | {"ats": "ashby", "id": "a", "company": "acme"},
              BASE | {"ats": "ashby", "id": "b", "company": "acme"}],
             db, "2026-01-01T00:00:00+00:00", covered=[("ashby", "acme")])
        _, _, closed = save([BASE | {"ats": "ashby", "id": "a", "company": "acme"}],
                            db, "2026-02-02T00:00:00+00:00", covered=None)
        assert closed == 0
        (open_rows,) = sqlite3.connect(db).execute(
            "SELECT COUNT(*) FROM jobs WHERE closed_at IS NULL").fetchone()
        assert open_rows == 2


def test_closing_is_scoped_to_boards_actually_scanned():
    """A --limit or --ats run must not close postings it never visited."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        save([BASE | {"ats": "ashby", "id": "a", "company": "acme"},
              BASE | {"ats": "greenhouse", "id": "z", "company": "acme"}],
             db, "2026-01-01T00:00:00+00:00",
             covered=[("ashby", "acme"), ("greenhouse", "acme")])
        # this run covered only ashby/acme, and saw nothing there
        _, _, closed = save([], db, "2026-02-02T00:00:00+00:00",
                            covered=[("ashby", "acme")])
        assert closed == 1
        got = dict(sqlite3.connect(db).execute("SELECT ats, closed_at FROM jobs"))
        assert got["ashby"] is not None
        assert got["greenhouse"] is None, \
            "same company name on another platform must be left alone"


def test_only_an_unfiltered_run_may_close_postings():
    """The highest-risk rule in the tool.

    closed_at means "this posting is gone". A run that filtered did not see the
    postings it filtered out, so it cannot make that claim. Miss one filter here and
    `--all --since 7d` silently marks everything older than a week as closed,
    destroying the fill-rate signal with no visible symptom.
    """
    from datetime import timedelta
    from job_boards import may_close_postings
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    assert may_close_postings(None, None, None, False), "a bare --all sees everything"

    assert not may_close_postings("engineer", None, None, False), "--title filters"
    assert not may_close_postings(None, re.compile("rust"), None, False), "--grep filters"
    assert not may_close_postings(None, None, cutoff, False), "--since filters"
    assert not may_close_postings(None, None, None, True), "--new-only filters"
    assert not may_close_postings("engineer", None, cutoff, True), "combinations filter"


def test_parse_duration():
    from job_boards import parse_duration
    assert parse_duration("7d") == 7
    assert parse_duration("2w") == 14
    assert parse_duration("3m") == 90
    assert parse_duration("1y") == 365
    assert parse_duration("90") == 90, "a bare number means days"
    assert parse_duration(" 7D ") == 7
    for junk in ("", "d", "-7", "7 days", "soon", "7x"):
        try:
            parse_duration(junk)
            raise AssertionError(f"{junk!r} should not parse")
        except ValueError:
            pass


def test_published_within_boundaries():
    from datetime import timedelta
    from job_boards import published_within
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    assert published_within((now - timedelta(days=1)).isoformat(), cutoff)
    assert published_within((now - timedelta(days=6, hours=23)).isoformat(), cutoff)
    assert not published_within((now - timedelta(days=8)).isoformat(), cutoff)
    # a Lever-style very old posting
    assert not published_within("2009-12-05T00:00:00+00:00", cutoff)
    # missing or malformed dates are excluded: --since promises freshness, so the
    # safe direction is to drop what cannot be shown to be fresh
    assert not published_within("", cutoff)
    assert not published_within("not a date", cutoff)


def test_since_filters_by_publish_date():
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    board = {"jobs": [
        {"id": "fresh", "title": "A", "isListed": True,
         "publishedAt": (now - timedelta(days=2)).isoformat()},
        {"id": "stale", "title": "B", "isListed": True,
         "publishedAt": (now - timedelta(days=400)).isoformat()},
    ]}
    cutoff = now - timedelta(days=7)
    rows = _with_fetch(board, lambda: scan_board(
        "ashby", "acme", None, False, "fuzzy", None, cutoff))
    assert [r["id"] for r in rows] == ["fresh"]
    # without a cutoff both survive
    both = _with_fetch(board, lambda: scan_board("ashby", "acme", None, False, "fuzzy"))
    assert len(both) == 2


def test_known_keys_reads_existing_and_legacy_databases():
    from job_boards import known_keys, save
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "t.db"
        assert known_keys(db) == set(), "a missing database has no known keys"
        save([BASE | {"ats": "lever", "id": "x", "company": "acme"}], db,
             "2026-01-01T00:00:00+00:00")
        assert known_keys(db) == {("lever", "x")}

        # a pre-multi-ATS database has no ats column; its rows are all Ashby
        legacy = Path(tmp) / "legacy.db"
        con = sqlite3.connect(legacy)
        con.executescript(
            "CREATE TABLE jobs (id TEXT PRIMARY KEY, company TEXT,"
            " first_seen TEXT, last_seen TEXT);"
        )
        con.execute("INSERT INTO jobs VALUES ('old','acme','t','t')")
        con.commit(); con.close()
        assert known_keys(legacy) == {("ashby", "old")}


def test_db_skips_rows_without_an_id():
    with tempfile.TemporaryDirectory() as tmp:
        assert save([BASE | {"ats": "ashby"}], Path(tmp) / "t.db",
                    "2026-01-01T00:00:00+00:00") == (0, 0, 0)


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #


def test_every_cli_flag_is_documented_in_the_readme():
    """Docs drift silently; this makes it a test failure.

    --since, --new-only and --refresh-recent all shipped before the agent
    instructions mentioned them, because nothing checked. A flag that exists and
    is undocumented is a feature nobody can find.
    """
    src = (Path(__file__).parent / "job_boards.py").read_text()
    flags = sorted(set(re.findall(r'p\.add_argument\(\s*"(--[a-z-]+)"', src)))
    assert len(flags) > 10, f"flag extraction looks broken, found {flags}"
    readme = (Path(__file__).parent / "README.md").read_text()
    missing = [f for f in flags if f not in readme]
    assert not missing, f"undocumented in README.md: {missing}"


def test_sort_recent_puts_the_newest_first():
    """With --since, alphabetical order buries the thing you asked for.

    A real run returned 5,980 postings from the last 24 hours; sorted by board, the
    six-minute-old one sat somewhere in the middle.
    """
    from job_boards import sort_rows
    rows = [
        BASE | {"ats": "lever", "company": "zz", "id": "old",
                "publishedAt": "2020-01-01T00:00:00+00:00"},
        BASE | {"ats": "ashby", "company": "aa", "id": "new",
                "publishedAt": "2026-07-27T23:00:00+00:00"},
        BASE | {"ats": "greenhouse", "company": "mm", "id": "mid",
                "publishedAt": "2026-07-01T00:00:00+00:00"},
        BASE | {"ats": "ashby", "company": "bb", "id": "undated", "publishedAt": ""},
    ]
    recent = list(rows)
    sort_rows(recent, "recent")
    assert [r["id"] for r in recent] == ["new", "mid", "old", "undated"], \
        "newest first, undated last"

    board = list(rows)
    sort_rows(board, "board")
    assert [r["id"] for r in board] == ["new", "undated", "mid", "old"], \
        "board order groups by ats then company"


def test_sort_recent_is_deterministic_within_a_timestamp():
    """Two postings sharing a timestamp must not reorder between runs."""
    from job_boards import sort_rows
    same = "2026-07-27T12:00:00+00:00"
    rows = [
        BASE | {"ats": "lever", "company": "zz", "id": "3", "publishedAt": same},
        BASE | {"ats": "ashby", "company": "bb", "id": "2", "publishedAt": same},
        BASE | {"ats": "ashby", "company": "aa", "id": "1", "publishedAt": same},
    ]
    first = list(rows); sort_rows(first, "recent")
    shuffled = [rows[2], rows[0], rows[1]]; sort_rows(shuffled, "recent")
    assert [r["id"] for r in first] == [r["id"] for r in shuffled] == ["1", "2", "3"]


def test_wiki_does_not_cite_tests_that_do_not_exist():
    """Generated docs can be confidently wrong, not just incomplete.

    A regeneration once listed `test_migrates_a_pre_multi_ats_database`, which has
    never existed — the real name is `test_migrates_a_single_ats_database`. Checking
    that a doc *mentions* something cannot catch that; checking that what it mentions
    is *real* can, and it is the cheap half of keeping generated prose honest.
    """
    here = Path(__file__).parent
    suite = set(re.findall(r"^def (test_\w+)", (here / "test_job_boards.py").read_text(), re.M))
    cited = set()
    for page in (here / "openwiki").rglob("*.md"):
        cited |= set(re.findall(r"\btest_\w+", page.read_text()))
    cited.discard("test_job_boards")  # the module, not a test
    invented = sorted(cited - suite)
    assert not invented, f"the wiki cites tests that do not exist: {invented}"


def test_agent_instructions_do_not_diverge():
    """AGENTS.md and CLAUDE.md carry the same hand-written guidance.

    Everything below the OPENWIKI marker is maintained by hand and duplicated
    across both files, so it drifts the moment someone edits one and not the
    other. OpenWiki owns the block above the marker and may legitimately differ.
    """
    marker = "<!-- OPENWIKI:END -->"
    here = Path(__file__).parent
    agents = (here / "AGENTS.md").read_text()
    claude = (here / "CLAUDE.md").read_text()
    assert marker in agents and marker in claude, "the OpenWiki marker went missing"
    assert agents.split(marker, 1)[1] == claude.split(marker, 1)[1], (
        "AGENTS.md and CLAUDE.md have diverged below the OpenWiki marker; "
        "copy one over the other"
    )


def test_both_filter_gates_are_documented_where_agents_will_see_them():
    """The two mistakes that corrupt data silently, so both must be findable.

    A filter missing from may_close_postings() makes a run assert that live postings
    are gone. A filter missing from may_use_etags() makes a later run skip a board
    whose postings were never persisted. Agents add filters; they read AGENTS.md.
    """
    here = Path(__file__).parent
    agents = (here / "AGENTS.md").read_text()
    for gate in ("may_close_postings", "may_use_etags"):
        assert gate in agents, f"AGENTS.md must tell a contributor to update {gate}()"
    assert "may_close_postings" in (here / "README.md").read_text()


def test_user_agent_is_header_safe():
    """http.client encodes headers as latin-1; non-ASCII breaks every request."""
    import job_boards
    job_boards.UA.encode("latin-1")
    assert job_boards.UA.isascii()


def test_csv_quoting():
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=FIELDS)
    w.writeheader()
    w.writerow(BASE | {
        "ats": "ashby", "company": "acme",
        "title": 'Senior Software Engineer, Backend "Core"',
        "location": "Remote – US • EU",
    })
    row = list(csv.DictReader(io.StringIO(buf.getvalue())))[0]
    assert row["title"] == 'Senior Software Engineer, Backend "Core"'
    assert row["location"] == "Remote – US • EU"


def test_failed_boards_round_trip_through_boards_from():
    """A failure file has to be readable as a board list, or the retry is manual.

    The count on stderr told you 8 boards failed but not which, so recovering them
    meant re-scanning all 13,000. The file is written in boards.json's shape
    precisely so `--boards-from <out>.failed.json` consumes it unchanged.
    """
    from job_boards import _read_boards
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "run.failed.json"
        failed = [("greenhouse", "yotpo"), ("greenhouse", "yieldmo"), ("ashby", "acme")]

        by_platform = {}
        for ats, slug in failed:
            by_platform.setdefault(ats, []).append(slug)
        path.write_text(json.dumps(by_platform, indent=2))

        assert _read_boards(path) == {
            "greenhouse": ["yotpo", "yieldmo"], "ashby": ["acme"]
        }, "the failure file must read back as a board list"


def test_a_failed_board_is_written_to_the_failure_file_and_a_clean_run_clears_it():
    """End to end, because this is the recovery path for lost data.

    A board that errors must land in <out>.failed.json; a run where nothing fails
    must delete a stale file rather than leave last run's failures readable as if
    they were this run's.
    """
    import sys
    import job_boards as jb

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "run"
        boards = Path(tmp) / "boards.json"
        boards.write_text(json.dumps({"ashby": ["good", "bad"]}))
        argv = [
            "job_boards.py", "--ats", "ashby", "--all", "--no-db",
            "--boards-from", str(boards), "--out", str(out),
        ]

        def scan(ats, slug, *a, **k):
            if slug == "bad":
                raise ValueError("boom")
            return []

        original_scan, original_argv = jb.scan_board, sys.argv
        jb.scan_board, sys.argv = scan, argv
        quiet = io.StringIO()          # main() narrates to stderr; keep the suite clean
        try:
            with contextlib.redirect_stderr(quiet):
                jb.main()
            failure_file = Path(f"{out}.failed.json")
            assert failure_file.exists(), "a failed board must be recorded, not just counted"
            assert json.loads(failure_file.read_text()) == {"ashby": ["bad"]}, \
                "only the board that failed belongs in the file"

            jb.scan_board = lambda ats, slug, *a, **k: []      # nothing fails now
            with contextlib.redirect_stderr(quiet):
                jb.main()
            assert not failure_file.exists(), \
                "a clean run must clear the file, or stale failures read as current"
        finally:
            jb.scan_board, sys.argv = original_scan, original_argv


def test_a_board_subset_still_may_not_widen_what_a_run_concludes():
    """--boards-from restricts boards, not postings, so the gates do not move.

    Closing is already scoped to the boards actually scanned — the same mechanism
    that stops --limit 10 from closing postings at thousands of unvisited
    companies. What may_close_postings guards is posting-level filters, and a
    board subset is not one.
    """
    from job_boards import may_close_postings, may_use_etags
    assert may_close_postings(None, None, None, False), \
        "an unfiltered run over a board subset still saw every posting on those boards"
    assert may_use_etags(None, None, None, False, True), \
        "and it still persisted every one of them"
def test_a_throttled_board_is_retried_not_dropped():
    """429 and 403 are 4xx, so they used to take the raise-immediately path.

    A real Greenhouse run lost 8 consecutive slugs to that: the board was thrown
    away for the whole run on the first refusal, with no backoff and no second
    chance. 404 must keep raising at once — that one means "not a customer", and
    retrying it would quadruple the cost of every dead slug.
    """
    import job_boards as jb

    original_request, original_sleep = jb._single_request, jb.time.sleep
    jb.time.sleep = lambda seconds: None        # keep the suite offline and instant
    try:
        for status in (429, 403):
            calls = []

            def refuse_twice(u, m, t, e=None, _s=status, _calls=calls):
                _calls.append(1)
                if len(_calls) <= 2:
                    return _s, {}, b""
                return 200, {}, b'{"jobs": []}'

            jb._single_request = refuse_twice
            assert jb.fetch("https://api.lever.co/x") == b'{"jobs": []}', \
                f"a {status} that clears on retry must still return the body"
            assert len(calls) == 3, f"expected 2 refusals then a success, got {len(calls)}"

        calls = []

        def always_404(u, m, t, e=None, _calls=calls):
            _calls.append(1)
            return 404, {}, b""

        jb._single_request = always_404
        raised = False
        try:
            jb.fetch("https://api.lever.co/x")
        except jb.NotFound:
            raised = True
        assert raised and len(calls) == 1, "404 must raise on the first attempt"
    finally:
        jb._single_request, jb.time.sleep = original_request, original_sleep


def test_retry_after_beats_the_exponential_delay_but_is_capped():
    """The server saying "wait 5s" is better information than 2**attempt.

    Capped, because a server asking for an hour would stall a 13,000-board run
    behind one slug; giving up and logging that board is the better trade.
    """
    from job_boards import _retry_delay
    assert _retry_delay("5", 0) == 5.0, "an explicit Retry-After wins"
    assert _retry_delay(None, 3) == 8.0, "no header falls back to 2**attempt"
    assert _retry_delay("garbage", 2) == 4.0, "an unparseable header falls back too"
    assert _retry_delay("3600", 0) == 30.0, "an absurd Retry-After is capped"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print(f"ok ({len(tests)} tests)")
