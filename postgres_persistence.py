#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2,<4"]
# ///
"""Persist scraper board results in Replit's managed PostgreSQL database.

This is intentionally separate from job_boards.py. It reuses that module's
platform URLs, HTTP client, payload selectors, and normalizers while retaining
the full description text before the CLI scraper discards its private field.

Examples:

    uv run postgres_persistence.py \
        --board ashby:abridge \
        --board ashby:linear \
        --board greenhouse:stripe

    # Enrich one Greenhouse board with full plain-text descriptions:
    uv run postgres_persistence.py \
        --board greenhouse:stripe \
        --greenhouse-content

    # Later, after the smoke test has been reviewed:
    uv run postgres_persistence.py --ats ashby --published-after 2026-07-15
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

import psycopg

import job_boards


ATS_NAMES = tuple(job_boards.SOURCES)

KONSTANTIN_EMAIL = "infobettor@gmail.com"
KONSTANTIN_PROFILE_TEXT = "Professional profile"
KONSTANTIN_CV_TEXT = """KONSTANTIN KONDEV
Program Manager

PROFESSIONAL SUMMARY
Results-driven Program Manager with 10+ years delivering complex software development projects and full-stack solutions. Proven track record building and scaling digital products from concept to market-ready platforms. Expertise in product & project management, agile methodologies, and stakeholder management across gaming & enterprise solution industries.

TOP SKILLS
Project Management
Product Management
People Management
JavaScript & Rest APIs & PostgreSQL

WORK HISTORY
CO-FOUNDER & COO 03/2026 till now
BlocksRace, Estepona, Spain
Led the creation of BlocksRace from concept to market-ready product, defining the vision, betting mechanics, user experience for a new prediction market category based on Bitcoin mining events.
Established relationships with casino operators, sportsbooks, gaming aggregators, and industry stakeholders to validate product-market fit and drive potential B2B integrations.
Oversaw product launch, user acquisition initiatives, community engagement, and platform analytics, using player behavior and betting data to continuously refine the product.

PROGRAM MANAGER 05/2024 to 02/2026
Playtech, Gibraltar, Gibraltar
Led cross-functional teams across Product, Development, Integration, Operations, Compliance, functions to deliver strategic initiatives for Tier-1 gaming operators.
Managed the successful implementation of Playtech's full technology portfolio, including PAM, Casino, Live Casino, Poker, Sportsbook, and third-party integrations.
Delivered complex platform launches and major product enhancements across regulated markets including Brazil, Ontario, Pennsylvania, Italy, Spain, Netherlands, and APAC.
Reduced project delivery timelines by 22% through process optimization, improved planning methodologies, and enhanced stakeholder coordination.
Built trusted relationships with executive-level client stakeholders, contributing to improved customer satisfaction and long-term partnership growth.
Streamlined communication and governance processes across internal and external stakeholders, increasing transparency, alignment, and project visibility.

SENIOR PROJECT MANAGER / TEAM LEAD 10/2019 to 05/2024
Playtech, Gibraltar
Managed complex software delivery projects from initiation through to launch, including the integration of new products, services, and third-party providers across multiple regulated markets.
Coordinated cross-functional teams spanning Commercial, Compliance, Product, Engineering, Integrations, Operations, and QA to ensure successful project execution.
Served as the primary point of contact for internal and external stakeholders, balancing business objectives, regulatory requirements, technical constraints, and delivery timelines.
Managed relationships with third-party technology, payment, and gaming content providers, overseeing integrations and operational readiness.
Team Lead of 3 project managers based in Estonia for Platform Projects Delivery.
Led and mentored a team of three Project Managers based in Estonia, supporting project delivery, prioritization and professional development.

AGILE PROJECT MANAGER 08/2018 to 07/2019
The Workshop - Inventors of play, Málaga
Managed the delivery of software development initiatives supporting online gaming and betting products, coordinating activities from project inception through to release.
Facilitated Agile and Scrum practices across multidisciplinary teams, ensuring effective planning, prioritization, execution, and continuous improvement.
Led Agile ceremonies including Roadmap Planning, Backlog Refinement, Sprint Planning, Daily Stand-ups, Reviews, and Retrospectives.
Worked closely with Product Managers, Designers, Solution Architects, Developers, QA Engineers, Delivery, and Support teams to remove impediments and drive successful outcomes.
Manage the relationship with 3rd party vendors and all stakeholders.

SENIOR PROJECT MANAGER 11/2015 to 08/2018
Hewlett Packard Enterprise, Sofia
Managing strategic and worldwide projects to ensure that they meet all scope, time, budget and quality expectations through planning, controlling and managing.
Identify, analyze and integrate business and technical needs.
Mitigate risks, perform impact analysis as part of the change management process and make recommendations regarding proposed changes to the projects.
Close monitoring of customer satisfaction, upselling /cross-selling.

PROJECT MANAGER 11/2013 to 10/2015
Hewlett Packard Enterprise, Sofia
Collaborated with infrastructure, platform, and technology consultancy teams to coordinate technical initiatives, manage dependencies, and support the delivery of scalable, high-availability services and infrastructure.
Plan and supervise all aspects of a project – overall and on a daily basis: planning, tasks completion, progress monitoring, budget, risk log etc.

PROJECT SUPPORT SPECIALIST 04/2013 to 11/2013
Hewlett Packard Enterprise, Sofia
Ensure the agreed project management methods, standards and processes are maintained throughout the project lifecycle.
Assist the Project Manager in the production and maintenance of project plans.
Set up and maintain systems for recording project costs.
Maintain risk and issue logs and change control records.

CUSTOMER/MERCHANT SERVICE REPRESENTATIVE 10/2010 to 04/2013
Paysafe (Skrill), Sofia
L1&L2 Customer Support.
Research and resolve complex customer issues.
Keep abreast of new company products and services.

FOUNDER 04/2009 to 05/2012
InfoBettor.com, Sofia
Conceptualized, developed, and successfully launched a sports betting information platform from initial idea to market-ready product.
Continuously delivered new features and platform enhancements based on user feedback and market analysis.
Revenue Growth & Partnerships: Established and scaled affiliate partnerships and sports betting operators.
Digital Marketing & User Acquisition: Designed and executed multi-channel marketing campaigns (Forums, FaceBook, Twitter, Google Ads).

CERTIFICATIONS
ITIL Foundation
Professional Scrum Master
Prince2

EDUCATION
University of National and World Economy 2004 - 2008
Bachelor of Business Administration Sofia - Bulgaria.

LANGUAGES
English (Proficient)
Bulgarian (Proficient)
Spanish (Basic)

TECHNICAL SKILLS & TOOLS
JavaScript, Node.js, Express.js, Rest API, SQL
Jira, Git, Monday"""

SCHEMA_DDL_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    company_id   BIGSERIAL PRIMARY KEY,
    display_name TEXT NOT NULL,
    industry     TEXT,
    category     TEXT,
    website_url  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_boards (
    board_id    BIGSERIAL PRIMARY KEY,
    company_id  BIGINT NOT NULL REFERENCES companies(company_id),
    ats         TEXT NOT NULL CHECK (ats IN ('ashby', 'greenhouse', 'lever')),
    slug        TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at   TIMESTAMPTZ,
    UNIQUE (ats, slug)
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id             BIGSERIAL PRIMARY KEY,
    board_id           BIGINT NOT NULL REFERENCES job_boards(board_id),
    company            TEXT,
    ats                TEXT NOT NULL CHECK (ats IN ('ashby', 'greenhouse', 'lever')),
    external_id        TEXT NOT NULL,
    title              TEXT NOT NULL,
    department         TEXT,
    team               TEXT,
    employment_type    TEXT,
    location_raw       TEXT,
    is_remote          BOOLEAN NOT NULL DEFAULT FALSE,
    workplace_type     TEXT,
    published_at       TIMESTAMPTZ,
    source_updated_at  TIMESTAMPTZ,
    job_url            TEXT,
    description_text   TEXT,
    first_seen         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen          TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at          TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ats, external_id)
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS company TEXT;

CREATE TABLE IF NOT EXISTS users (
    user_id                 BIGSERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    email                   TEXT UNIQUE NOT NULL,
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    is_default              BOOLEAN NOT NULL DEFAULT FALSE,
    profile_text            TEXT,
    cv_text                 TEXT,
    target_roles            TEXT[],
    target_industries       TEXT[],
    base_city               TEXT,
    base_country            TEXT,
    base_latitude           NUMERIC(9,6),
    base_longitude          NUMERIC(9,6),
    remote_allowed          BOOLEAN,
    onsite_allowed          BOOLEAN,
    onsite_max_distance_km  INTEGER,
    hybrid_allowed          BOOLEAN,
    hybrid_max_distance_km  INTEGER,
    willing_to_relocate     BOOLEAN,
    relocation_cities       TEXT[],
    relocation_countries    TEXT[],
    preferred_regions       TEXT[],
    excluded_regions        TEXT[],
    min_match_score         INTEGER,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_matches (
    match_id       BIGSERIAL PRIMARY KEY,
    user_id        BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id         BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    score          INTEGER,
    match_status   TEXT NOT NULL DEFAULT 'pending',
    feedback       TEXT,
    model_name     TEXT,
    model_version  TEXT,
    notified_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_matches_user_job_key UNIQUE (user_id, job_id)
);

CREATE INDEX IF NOT EXISTS jobs_published_at_idx
    ON jobs (published_at DESC);
CREATE INDEX IF NOT EXISTS jobs_remote_idx
    ON jobs (is_remote);
CREATE INDEX IF NOT EXISTS jobs_workplace_type_idx
    ON jobs (workplace_type);
CREATE INDEX IF NOT EXISTS jobs_board_id_idx
    ON jobs (board_id);
CREATE INDEX IF NOT EXISTS jobs_closed_at_idx
    ON jobs (closed_at);
CREATE INDEX IF NOT EXISTS jobs_title_lower_idx
    ON jobs (lower(title));
CREATE INDEX IF NOT EXISTS jobs_company_idx
    ON jobs (company);
CREATE INDEX IF NOT EXISTS job_boards_company_id_idx
    ON job_boards (company_id);
CREATE INDEX IF NOT EXISTS job_boards_ats_idx
    ON job_boards (ats);
CREATE INDEX IF NOT EXISTS companies_category_idx
    ON companies (category);
CREATE INDEX IF NOT EXISTS companies_industry_idx
    ON companies (industry);
CREATE INDEX IF NOT EXISTS job_matches_user_id_idx
    ON job_matches (user_id);
CREATE INDEX IF NOT EXISTS job_matches_job_id_idx
    ON job_matches (job_id);
CREATE INDEX IF NOT EXISTS job_matches_score_idx
    ON job_matches (score);
CREATE INDEX IF NOT EXISTS job_matches_match_status_idx
    ON job_matches (match_status);
CREATE INDEX IF NOT EXISTS job_matches_notified_at_idx
    ON job_matches (notified_at);

UPDATE jobs AS j
SET company = c.display_name
FROM job_boards AS b
JOIN companies AS c ON c.company_id = b.company_id
WHERE j.board_id = b.board_id
  AND j.company IS DISTINCT FROM c.display_name;
"""

KONSTANTIN_TARGET_ROLES = (
    "Project Manager",
    "Product Manager",
    "Program Manager",
    "Technical Project Manager",
    "Technical Program Manager",
    "Product Owner",
    "Product Operations",
    "Implementation Manager",
    "Delivery Manager",
    "Operations Manager",
)
KONSTANTIN_TARGET_INDUSTRIES = (
    "Gambling",
    "Crypto",
    "Fintech",
    "Finance",
    "Technology",
)
KONSTANTIN_RELOCATION_CITIES = ("Madrid", "Málaga")
KONSTANTIN_RELOCATION_COUNTRIES = ("Spain", "Portugal")
KONSTANTIN_PREFERRED_REGIONS = ("Europe", "EU", "EEA", "EMEA")
KONSTANTIN_EXCLUDED_REGIONS = ("US-only",)


def _sql_literal(value: str) -> str:
    """Quote a trusted static seed value for the schema initialization SQL."""
    return "'" + value.replace("'", "''") + "'"


def _sql_text_array(values: tuple[str, ...]) -> str:
    return "ARRAY[" + ", ".join(_sql_literal(value) for value in values) + "]::TEXT[]"


KONSTANTIN_SEED_SQL = f"""
INSERT INTO users (
    name, email, active, is_default, profile_text, cv_text, target_roles,
    target_industries, base_city, base_country, base_latitude, base_longitude,
    remote_allowed, onsite_allowed, onsite_max_distance_km, hybrid_allowed,
    hybrid_max_distance_km, willing_to_relocate, relocation_cities,
    relocation_countries, preferred_regions, excluded_regions, min_match_score
) VALUES (
    {_sql_literal("Konstantin Kondev")},
    {_sql_literal(KONSTANTIN_EMAIL)},
    TRUE,
    TRUE,
    {_sql_literal(KONSTANTIN_PROFILE_TEXT)},
    {_sql_literal(KONSTANTIN_CV_TEXT)},
    {_sql_text_array(KONSTANTIN_TARGET_ROLES)},
    {_sql_text_array(KONSTANTIN_TARGET_INDUSTRIES)},
    'Estepona',
    'Spain',
    NULL,
    NULL,
    TRUE,
    TRUE,
    100,
    TRUE,
    600,
    TRUE,
    {_sql_text_array(KONSTANTIN_RELOCATION_CITIES)},
    {_sql_text_array(KONSTANTIN_RELOCATION_COUNTRIES)},
    {_sql_text_array(KONSTANTIN_PREFERRED_REGIONS)},
    {_sql_text_array(KONSTANTIN_EXCLUDED_REGIONS)},
    75
)
ON CONFLICT (email) DO UPDATE SET
    name = EXCLUDED.name,
    active = EXCLUDED.active,
    is_default = EXCLUDED.is_default,
    profile_text = EXCLUDED.profile_text,
    cv_text = EXCLUDED.cv_text,
    target_roles = EXCLUDED.target_roles,
    target_industries = EXCLUDED.target_industries,
    base_city = EXCLUDED.base_city,
    base_country = EXCLUDED.base_country,
    base_latitude = EXCLUDED.base_latitude,
    base_longitude = EXCLUDED.base_longitude,
    remote_allowed = EXCLUDED.remote_allowed,
    onsite_allowed = EXCLUDED.onsite_allowed,
    onsite_max_distance_km = EXCLUDED.onsite_max_distance_km,
    hybrid_allowed = EXCLUDED.hybrid_allowed,
    hybrid_max_distance_km = EXCLUDED.hybrid_max_distance_km,
    willing_to_relocate = EXCLUDED.willing_to_relocate,
    relocation_cities = EXCLUDED.relocation_cities,
    relocation_countries = EXCLUDED.relocation_countries,
    preferred_regions = EXCLUDED.preferred_regions,
    excluded_regions = EXCLUDED.excluded_regions,
    min_match_score = EXCLUDED.min_match_score,
    updated_at = now();
"""

# Keep one public schema command for callers and tests. The seed is deliberately
# part of initialization so any importer run repairs the canonical seed without
# duplicating it or touching existing jobs/matches.
SCHEMA_SQL = SCHEMA_DDL_SQL + KONSTANTIN_SEED_SQL


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _timestamp(value: Any) -> datetime | None:
    """Convert ISO strings or epoch milliseconds to timezone-aware datetimes."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        # Lever uses epoch milliseconds for createdAt and some providers use the
        # same representation for updated timestamps.
        seconds = value / 1000 if abs(value) > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _source_updated_at(ats: str, raw_job: dict[str, Any]) -> datetime | None:
    """Read only an upstream modified/updated field when that ATS supplies one."""
    keys_by_ats = {
        "ashby": ("updatedAt", "updated_at", "modifiedAt", "modified_at"),
        "greenhouse": ("updated_at", "updatedAt", "modified_at", "modifiedAt"),
        "lever": ("updatedAt", "updated_at", "modifiedAt", "modified_at"),
    }
    for key in keys_by_ats[ats]:
        if raw_job.get(key) not in (None, ""):
            return _timestamp(raw_job[key])
    return None


def _metadata_text(value: Any) -> str | None:
    if isinstance(value, list):
        parts = [_metadata_text(item) for item in value]
        return _optional(", ".join(part for part in parts if part))
    if isinstance(value, dict):
        for key in ("name", "value", "label"):
            if value.get(key) not in (None, ""):
                return _metadata_text(value[key])
        return None
    return _optional(value)


def _greenhouse_metadata_field(
    raw_job: dict[str, Any],
    field: str,
) -> str | None:
    """Read an explicitly named Greenhouse custom field without guessing."""
    aliases = {
        "department": {"department", "department name", "dept"},
        "team": {"team", "team name"},
    }[field]
    direct = _optional(raw_job.get(field))
    if direct:
        return direct
    metadata = raw_job.get("metadata")
    if not isinstance(metadata, list):
        return None
    for item in metadata:
        if not isinstance(item, dict):
            continue
        name = _optional(item.get("name"))
        if name and name.casefold().strip() in aliases:
            value = _metadata_text(item.get("value"))
            if value:
                return value
    return None


def _plain_description(
    ats: str,
    normalized: dict[str, Any],
    greenhouse_content: bool = False,
) -> str | None:
    """Return the full description that the existing adapter already received."""
    if ats == "greenhouse" and not greenhouse_content:
        # The normal persistence path deliberately does not request content=true.
        # Keep this nullable until a separate enrichment strategy is approved.
        return None
    description = normalized.get("_description") or ""
    # Greenhouse content is HTML-escaped inside the JSON string (`&lt;p&gt;`).
    # Decode once before the shared cleaner strips tags; otherwise those tags
    # become visible only after the cleaner has already run.
    text = job_boards.plain_text(unescape(str(description)))
    return text or None


def _board_specs(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.board:
        specs: list[tuple[str, str]] = []
        for value in args.board:
            ats, separator, slug = value.partition(":")
            if not separator or ats not in ATS_NAMES or not slug.strip():
                raise SystemExit(
                    f"--board must use ATS:SLUG with ATS one of {', '.join(ATS_NAMES)}"
                )
            specs.append((ats, slug.strip()))
        if args.ats != "all":
            selected = set(_ats_list(args.ats))
            specs = [spec for spec in specs if spec[0] in selected]
        if not specs:
            raise SystemExit("the requested --board values do not match --ats")
        return list(dict.fromkeys(specs))

    ats_list = _ats_list(args.ats)
    if args.boards_from:
        path = Path(args.boards_from)
        if not path.is_absolute():
            path = job_boards.HERE / path
        boards = job_boards._read_boards(path)
    else:
        boards = job_boards.load_boards(False, ats_list)
    if not boards:
        raise SystemExit("no boards found")
    specs = [
        (ats, slug)
        for ats in ats_list
        for slug in boards.get(ats, [])
    ]
    if args.limit:
        specs = [
            spec for ats in ats_list
            for spec in [(ats, slug) for slug in boards.get(ats, [])[: args.limit]]
        ]
    if not specs:
        raise SystemExit("no boards found for the selected ATS")
    return specs


def _ats_list(value: str) -> list[str]:
    ats_list = list(ATS_NAMES) if value == "all" else [
        item.strip() for item in value.split(",") if item.strip()
    ]
    unknown = [item for item in ats_list if item not in ATS_NAMES]
    if unknown:
        raise SystemExit(
            f"unknown --ats {', '.join(unknown)}; choose from {', '.join(ATS_NAMES)}"
        )
    return ats_list


def _fetch_normalized(
    ats: str,
    slug: str,
    published_after: datetime | None = None,
    greenhouse_content: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    payload = json.loads(
        job_boards.fetch(
            job_boards.board_url(
                ats,
                slug,
                want_content=greenhouse_content and ats == "greenhouse",
            ),
            timeout=30,
        )
    )
    source = job_boards.SOURCES[ats]
    raw_jobs = source["jobs"](payload)
    if not isinstance(raw_jobs, list):
        raise ValueError(f"{ats}/{slug}: response has no jobs array")

    normalized_rows: list[dict[str, Any]] = []
    skipped = 0
    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            continue
        normalized = source["normalize"](raw_job)
        if normalized is None:
            continue
        normalized = job_boards._clean(normalized)
        published_at = _timestamp(normalized.get("publishedAt"))
        if published_after is not None and (
            published_at is None or published_at < published_after
        ):
            skipped += 1
            continue
        normalized_rows.append({
            "ats": ats,
            "slug": slug,
            "external_id": _optional(normalized.get("id")),
            "title": str(normalized.get("title") or ""),
            "department": (
                _greenhouse_metadata_field(raw_job, "department")
                if ats == "greenhouse"
                else _optional(normalized.get("department"))
            ),
            "team": (
                _greenhouse_metadata_field(raw_job, "team")
                if ats == "greenhouse"
                else _optional(normalized.get("team"))
            ),
            "employment_type": _optional(normalized.get("employmentType")),
            "location_raw": _optional(normalized.get("location")),
            "is_remote": bool(normalized.get("isRemote")),
            "workplace_type": _optional(normalized.get("workplaceType")),
            "published_at": published_at,
            "source_updated_at": _source_updated_at(ats, raw_job),
            "job_url": _optional(normalized.get("jobUrl")),
            "description_text": _plain_description(
                ats,
                normalized,
                greenhouse_content=greenhouse_content,
            ),
        })
    rows = [row for row in normalized_rows if row["external_id"]]
    return rows, skipped


def _ensure_board(
    cur: psycopg.Cursor,
    ats: str,
    slug: str,
    seen_at: datetime,
) -> int:
    cur.execute(
        "SELECT board_id FROM job_boards WHERE ats = %s AND slug = %s",
        (ats, slug),
    )
    existing = cur.fetchone()
    if existing:
        board_id = existing[0]
        cur.execute(
            "UPDATE job_boards SET last_seen = %s, closed_at = NULL "
            "WHERE board_id = %s",
            (seen_at, board_id),
        )
        return board_id

    # One source-scoped company placeholder per board is deliberate. There is
    # no automatic claim that two identical names/slugs across ATSs are one
    # legal company; enrichment or manual linking can happen later.
    cur.execute(
        "INSERT INTO companies (display_name, created_at, updated_at) "
        "VALUES (%s, %s, %s) RETURNING company_id",
        (slug, seen_at, seen_at),
    )
    company_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO job_boards "
        "(company_id, ats, slug, source_url, first_seen, last_seen) "
        "VALUES (%s, %s, %s, %s, %s, %s) RETURNING board_id",
        (
            company_id,
            ats,
            slug,
            job_boards.board_url(ats, slug),
            seen_at,
            seen_at,
        ),
    )
    return cur.fetchone()[0]


def _board_company_name(cur: psycopg.Cursor, board_id: int) -> str:
    cur.execute(
        "SELECT c.display_name "
        "FROM job_boards AS b "
        "JOIN companies AS c ON c.company_id = b.company_id "
        "WHERE b.board_id = %s",
        (board_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"board {board_id} has no company")
    return row[0]


def _upsert_board_jobs(
    cur: psycopg.Cursor,
    board_id: int,
    company: str,
    rows: list[dict[str, Any]],
    seen_at: datetime,
    close_missing: bool = True,
) -> tuple[int, int]:
    seen_ids = {row["external_id"] for row in rows}
    for row in rows:
        cur.execute(
            """
            INSERT INTO jobs (
                board_id, company, ats, external_id, title, department, team,
                employment_type, location_raw, is_remote, workplace_type,
                published_at, source_updated_at, job_url, description_text,
                first_seen, last_seen, updated_at
            ) VALUES (
                %(board_id)s, %(company)s, %(ats)s, %(external_id)s, %(title)s,
                %(department)s, %(team)s, %(employment_type)s,
                %(location_raw)s, %(is_remote)s, %(workplace_type)s,
                %(published_at)s, %(source_updated_at)s, %(job_url)s,
                %(description_text)s, %(seen_at)s, %(seen_at)s, %(seen_at)s
            )
            ON CONFLICT (ats, external_id) DO UPDATE SET
                board_id = EXCLUDED.board_id,
                company = EXCLUDED.company,
                title = EXCLUDED.title,
                department = EXCLUDED.department,
                team = EXCLUDED.team,
                employment_type = EXCLUDED.employment_type,
                location_raw = EXCLUDED.location_raw,
                is_remote = EXCLUDED.is_remote,
                workplace_type = EXCLUDED.workplace_type,
                published_at = COALESCE(EXCLUDED.published_at, jobs.published_at),
                source_updated_at = COALESCE(
                    EXCLUDED.source_updated_at, jobs.source_updated_at
                ),
                job_url = EXCLUDED.job_url,
                description_text = COALESCE(
                    EXCLUDED.description_text, jobs.description_text
                ),
                last_seen = EXCLUDED.last_seen,
                closed_at = NULL,
                updated_at = EXCLUDED.updated_at
            """,
            {**row, "board_id": board_id, "company": company, "seen_at": seen_at},
        )

    closed = 0
    if close_missing:
        cur.execute(
            "UPDATE jobs SET closed_at = %s, updated_at = %s "
            "WHERE board_id = %s AND closed_at IS NULL AND last_seen < %s",
            (seen_at, seen_at, board_id, seen_at),
        )
        closed = cur.rowcount
    return len(seen_ids), closed


def _count_existing(
    cur: psycopg.Cursor,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    ats_values = {row["ats"] for row in rows}
    if len(ats_values) != 1:
        raise ValueError("existing-job lookup expects rows from one ATS")
    ats = next(iter(ats_values))
    cur.execute(
        "SELECT COUNT(*) FROM jobs "
        "WHERE ats = %s AND external_id = ANY(%s)",
        (ats, [row["external_id"] for row in rows]),
    )
    return cur.fetchone()[0]


def run(args: argparse.Namespace) -> int:
    specs = _board_specs(args)
    published_after = _timestamp(args.published_after) if args.published_after else None
    if args.published_after and published_after is None:
        raise SystemExit(
            "--published-after must be an ISO date or timestamp, such as 2026-07-15"
        )
    greenhouse_content = args.greenhouse_content
    if greenhouse_content and not any(ats == "greenhouse" for ats, _ in specs):
        raise SystemExit("--greenhouse-content requires at least one Greenhouse board")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")

    total_jobs = 0
    total_new = 0
    total_updated = 0
    total_closed = 0
    failed = 0

    with psycopg.connect(dsn) as conn:
        conn.execute(SCHEMA_SQL)
        for index, (ats, slug) in enumerate(specs, 1):
            try:
                rows, skipped = _fetch_normalized(
                    ats,
                    slug,
                    published_after,
                    greenhouse_content=greenhouse_content,
                )
                with conn.transaction():
                    with conn.cursor() as cur:
                        seen_at = datetime.now(timezone.utc)
                        board_id = _ensure_board(cur, ats, slug, seen_at)
                        company = _board_company_name(cur, board_id)
                        existing = _count_existing(cur, rows)
                        _, closed = _upsert_board_jobs(
                            cur,
                            board_id,
                            company,
                            rows,
                            seen_at,
                            close_missing=published_after is None,
                        )
                new = len(rows) - existing
                total_jobs += len(rows)
                total_new += new
                total_updated += existing
                total_closed += closed
                print(
                    f"{index}/{len(specs)} {ats}/{slug}: "
                    f"{len(rows)} jobs ({new} new, {existing} updated, "
                    f"{skipped} before cutoff, {closed} closed)"
                )
            except job_boards.NotFound:
                failed += 1
                print(f"{index}/{len(specs)} {ats}/{slug}: 404", file=sys.stderr)
            except Exception as exc:
                failed += 1
                print(
                    f"{index}/{len(specs)} {ats}/{slug}: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

    print(
        f"\nPostgreSQL import complete: {total_jobs} current jobs, "
        f"{total_new} new, {total_updated} updated, {total_closed} closed, "
        f"{failed} failed boards"
    )
    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persist selected job boards in Replit PostgreSQL."
    )
    parser.add_argument(
        "--ats",
        default="all",
        help="comma-separated ATS platforms when selecting cached boards",
    )
    parser.add_argument(
        "--board",
        action="append",
        help="exact board to import, in ATS:SLUG form; repeatable",
    )
    parser.add_argument(
        "--boards-from",
        help="boards.json-shaped file to import instead of the local registry",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="maximum boards per selected ATS when using cached boards",
    )
    parser.add_argument(
        "--published-after",
        help=(
            "persist only jobs published on or after this inclusive ISO date "
            "or timestamp"
        ),
    )
    parser.add_argument(
        "--greenhouse-content",
        action="store_true",
        help=(
            "request Greenhouse content=true and persist full plain-text "
            "descriptions; use with a scoped board list because responses are larger"
        ),
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()