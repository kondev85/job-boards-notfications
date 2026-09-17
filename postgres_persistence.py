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

    # Daily lightweight Ashby + Greenhouse + Lever scan, match, and reports:
    uv run postgres_persistence.py --daily --published-after 2026-08-26

    # Daily scan for one ATS only:
    uv run postgres_persistence.py --daily --ats lever --published-after 2026-08-26
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sys
import unicodedata
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

import gemini_service
import job_boards


ATS_NAMES = tuple(job_boards.SOURCES)
DAILY_IMPORT_LEASE_SECONDS = 15 * 60
DAILY_BOARD_WORKERS = 4
DAILY_DETAIL_CONCURRENCY = 3
DAILY_MIN_SUCCESS_PERCENT = 98
DAILY_ATS_CONCURRENCY = {
    "workday": 1,
    "smartrecruiters": 1,
    "recruitee": 2,
    "teamtailor": 2,
    "workable": 1,
}


def _daily_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"

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
    ats         TEXT NOT NULL CHECK (ats IN (
        'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
        'recruitee', 'teamtailor', 'workable'
    )),
    slug        TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    source_url  TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at   TIMESTAMPTZ,
    etag         TEXT,
    etag_seen_at TIMESTAMPTZ,
    etag_published_after TIMESTAMPTZ,
    UNIQUE (ats, slug)
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id             BIGSERIAL PRIMARY KEY,
    board_id           BIGINT NOT NULL REFERENCES job_boards(board_id),
    company            TEXT,
    ats                TEXT NOT NULL CHECK (ats IN (
        'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
        'recruitee', 'teamtailor', 'workable'
    )),
    external_id        TEXT NOT NULL,
    title              TEXT NOT NULL,
    department         TEXT,
    team               TEXT,
    employment_type    TEXT,
    location_raw       TEXT,
    address            JSONB,
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
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address JSONB;
ALTER TABLE job_boards ADD COLUMN IF NOT EXISTS etag TEXT;
ALTER TABLE job_boards ADD COLUMN IF NOT EXISTS etag_seen_at TIMESTAMPTZ;
ALTER TABLE job_boards ADD COLUMN IF NOT EXISTS etag_published_after TIMESTAMPTZ;
ALTER TABLE job_boards
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE job_boards DROP CONSTRAINT IF EXISTS job_boards_ats_check;
ALTER TABLE job_boards ADD CONSTRAINT job_boards_ats_check
    CHECK (ats IN (
        'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
        'recruitee', 'teamtailor', 'workable'
    ));
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_ats_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_ats_check
    CHECK (ats IN (
        'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
        'recruitee', 'teamtailor', 'workable'
    ));

CREATE TABLE IF NOT EXISTS users (
    user_id                 BIGSERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    email                   TEXT UNIQUE NOT NULL,
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    is_default              BOOLEAN NOT NULL DEFAULT FALSE,
    profile_text            TEXT,
    cv_text                 TEXT,
    profile_json            JSONB,
    profile_generated_at    TIMESTAMPTZ,
    profile_model           TEXT,
    profile_version         TEXT,
    profile_source_hash     TEXT,
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
    min_match_score         INTEGER,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_json JSONB;
ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_generated_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_model TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_version TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_source_hash TEXT;
ALTER TABLE users DROP COLUMN IF EXISTS preferred_regions;
ALTER TABLE users DROP COLUMN IF EXISTS excluded_regions;

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

CREATE TABLE IF NOT EXISTS daily_import_runs (
    import_run_id              BIGSERIAL PRIMARY KEY,
    ats_scope                  TEXT[] NOT NULL,
    published_after            TIMESTAMPTZ NOT NULL,
    status                     TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN (
                                   'running', 'completed', 'completed_with_errors'
                               )),
    started_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at           TIMESTAMPTZ,
    worker_id                  TEXT,
    completed_at               TIMESTAMPTZ,
    board_count                INTEGER NOT NULL,
    failed_board_count         INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE daily_import_runs
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE daily_import_runs
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE daily_import_runs
    ADD COLUMN IF NOT EXISTS worker_id TEXT;
CREATE INDEX IF NOT EXISTS daily_import_runs_resume_idx
    ON daily_import_runs (ats_scope, started_at)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS daily_import_run_boards (
    import_run_id  BIGINT NOT NULL
                   REFERENCES daily_import_runs(import_run_id) ON DELETE CASCADE,
    board_id       BIGINT NOT NULL REFERENCES job_boards(board_id),
    position       INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'completed', 'empty', 'failed')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    jobs_fetched   INTEGER NOT NULL DEFAULT 0,
    jobs_inserted  INTEGER NOT NULL DEFAULT 0,
    jobs_updated   INTEGER NOT NULL DEFAULT 0,
    error_type     TEXT,
    error_message  TEXT,
    retry_after    TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    PRIMARY KEY (import_run_id, board_id),
    UNIQUE (import_run_id, position)
);

ALTER TABLE daily_import_run_boards
    ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS daily_import_run_boards_status_idx
    ON daily_import_run_boards (import_run_id, status, position);
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

ALTER TABLE users ADD COLUMN IF NOT EXISTS clerk_user_id TEXT UNIQUE;

CREATE TABLE IF NOT EXISTS user_job_state (
    user_id    BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id     BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
     status     TEXT NOT NULL DEFAULT 'new'
                CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected')),
    viewed_at  TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, job_id)
);
ALTER TABLE user_job_state ADD COLUMN IF NOT EXISTS viewed_at TIMESTAMPTZ;
ALTER TABLE user_job_state DROP CONSTRAINT IF EXISTS user_job_state_status_check;
ALTER TABLE user_job_state
    ADD CONSTRAINT user_job_state_status_check
    CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected'));

CREATE TABLE IF NOT EXISTS user_job_status_history (
    history_id BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id     BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
     status     TEXT NOT NULL
                CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE user_job_status_history DROP CONSTRAINT IF EXISTS user_job_status_history_status_check;
ALTER TABLE user_job_status_history
    ADD CONSTRAINT user_job_status_history_status_check
    CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected'));

CREATE TABLE IF NOT EXISTS feedback_signals (
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    signal  TEXT NOT NULL CHECK (signal IN ('saved', 'applied', 'rejected')),
    weight  INTEGER NOT NULL,
    PRIMARY KEY (user_id, signal)
);

INSERT INTO feedback_signals (user_id, signal, weight)
SELECT user_id, signal, weight
FROM users
CROSS JOIN (
    VALUES ('saved', 3), ('applied', 1), ('rejected', -2)
) AS defaults(signal, weight)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS profile_recommendation_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    user_id             BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    profile_source_hash TEXT NOT NULL,
    published_after     TIMESTAMPTZ,
    candidate_floor     INTEGER NOT NULL,
    top_n               INTEGER NOT NULL,
    candidate_count     INTEGER NOT NULL,
    shortlist_hash      TEXT NOT NULL,
    model_name          TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS profile_recommendation_runs_user_idx
    ON profile_recommendation_runs (user_id, created_at DESC);
ALTER TABLE profile_recommendation_runs
    ADD COLUMN IF NOT EXISTS published_after TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS job_profile_reviews (
    review_id              BIGSERIAL PRIMARY KEY,
    user_id                BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_id                 BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    profile_source_hash    TEXT NOT NULL,
    job_source_hash        TEXT NOT NULL,
    prompt_version         TEXT NOT NULL,
    gemini_model           TEXT NOT NULL,
    batch_fit_score        INTEGER CHECK (batch_fit_score BETWEEN 0 AND 100),
    batch_recommendation   TEXT,
    batch_strengths        JSONB,
    batch_concerns         JSONB,
    batch_rationale        TEXT,
    batch_reviewed_at      TIMESTAMPTZ,
    recommendation_run_id  BIGINT REFERENCES profile_recommendation_runs(run_id)
                           ON DELETE SET NULL,
    final_fit_score        INTEGER CHECK (final_fit_score BETWEEN 0 AND 100),
    final_rank              INTEGER,
    final_recommendation   TEXT,
    final_strengths        JSONB,
    final_concerns         JSONB,
    final_rationale        TEXT,
    final_reviewed_at      TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_profile_reviews_user_job_key UNIQUE (user_id, job_id)
);

CREATE INDEX IF NOT EXISTS job_profile_reviews_user_idx
    ON job_profile_reviews (user_id);
CREATE INDEX IF NOT EXISTS job_profile_reviews_run_idx
    ON job_profile_reviews (recommendation_run_id);

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

MATCH_MODEL_NAME = "deterministic-preferences"
MATCH_MODEL_VERSION = "v3-concrete-location"
RECOMMENDATION_MODEL_NAME = "gemini-profile-reranker"
RECOMMENDATION_MODEL_VERSION = gemini_service.JOB_REVIEW_VERSION
DEFAULT_RECOMMENDATION_FLOOR = 55
RECOMMENDATION_GEMINI_REVIEW_FLOOR = 75
RECOMMENDATION_MAX_GEMINI_CANDIDATES = 20
RECOMMENDATION_PINNED_FINAL_FLOOR = 70
RECOMMENDATION_BATCH_SIZE = 10
RECOMMENDATION_SHORTLIST_SIZE = 5
RECOMMENDATION_TOP_N = 5
MATCH_SCORE_WEIGHTS = {
    "role": 40,
    "industry": 25,
    "capabilities": 20,
    "seniority": 15,
}

_PROFILE_STRENGTH_WEIGHTS = {
    "strong": 1.0,
    "moderate": 0.8,
    "limited": 0.55,
    "incidental": 0.3,
    "exposure": 0.45,
    "evidence_only": 0.65,
}
_PROFILE_PROFICIENCY_WEIGHTS = {
    "strong": 1.0,
    "working_knowledge": 0.8,
    "exposure": 0.45,
    "training_only": 0.25,
}
_PROFILE_RECENCY_WEIGHTS = {
    "current": 1.0,
    "recent": 0.95,
    "older": 0.75,
    "unknown": 0.85,
}
_MATCH_WORD_ALIASES = {
    "programme": "program",
    "management": "manager",
    "managerial": "manager",
    "leadership": "leader",
    "engineering": "engineer",
    "operations": "operation",
    "operational": "operation",
}
_SENIORITY_RANKS = (
    ("chief executive officer", 7),
    ("chief operating officer", 7),
    ("chief product officer", 7),
    ("chief technology officer", 7),
    ("vice president", 6),
    ("vp", 6),
    ("director", 6),
    ("head", 6),
    ("principal", 5),
    ("staff", 5),
    ("lead", 5),
    ("senior", 4),
    ("manager", 4),
    ("mid", 3),
    ("associate", 2),
    ("junior", 1),
    ("entry", 1),
    ("intern", 0),
)


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
    relocation_countries, min_match_score
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
    55
)
ON CONFLICT (email) DO NOTHING;
"""

# Keep one public schema command for callers and tests. The seed is deliberately
# part of initialization so a fresh database gets the canonical profile, while
# existing users' preferences remain authoritative on later importer runs.
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
    if text.endswith(" UTC"):
        text = text[:-4] + "+00:00"
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
        "smartrecruiters": ("updatedDate", "updatedAt", "updated_at", "modifiedAt"),
        "workday": ("updatedAt", "updated_at", "modifiedAt", "modified_at"),
        "recruitee": ("updated_at", "updatedAt", "modified_at", "modifiedAt"),
        "teamtailor": ("date_modified", "dateModified", "updated_at", "updatedAt"),
        "workable": ("updated_at", "updatedAt", "modified_at", "modifiedAt"),
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
        boards = job_boards.load_boards(
            False,
            ats_list,
            workday_bruteforce=getattr(args, "workday_bruteforce", False),
        )
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


def _database_board_specs(
    conn: psycopg.Connection,
    ats_list: list[str],
) -> list[tuple[str, str]]:
    """Return every enabled, open board persisted for the selected ATSes."""
    rows = conn.execute(
        """
        SELECT ats, slug
        FROM job_boards
        WHERE ats = ANY(%s)
          AND active IS TRUE
          AND closed_at IS NULL
        ORDER BY ats, slug
        """,
        (ats_list,),
    ).fetchall()
    return [(ats, slug) for ats, slug in rows]


def _database_has_board_rows(
    conn: psycopg.Connection,
    ats_list: list[str],
) -> bool:
    """Whether PostgreSQL has a board registry for the selected ATSes.

    This distinguishes an empty registry from a registry where an administrator
    intentionally disabled every board. The latter must not fall back to the
    local seed and fetch disabled boards.
    """
    return bool(
        conn.execute(
            "SELECT EXISTS(SELECT 1 FROM job_boards WHERE ats = ANY(%s))",
            (ats_list,),
        ).fetchone()[0]
    )


def _prepare_daily_import_run(
    conn: psycopg.Connection,
    ats_scope: list[str],
    published_after: datetime,
    *,
    auto_resume: bool = False,
    worker_id: str | None = None,
) -> tuple[int, list[tuple[int, int, str, str]], bool, int, datetime]:
    """Resume an incomplete run or snapshot a new ordered board list.

    Manual runs retain exact scope/cutoff matching. Automatic scheduled runs
    reclaim the oldest expired lease for the scope and continue using that
    run's original cutoff, preserving the run's reproducibility.
    """
    canonical_scope = sorted(set(ats_scope))
    lease_seconds = DAILY_IMPORT_LEASE_SECONDS
    with conn.transaction():
        if auto_resume:
            existing = conn.execute(
                """
                SELECT import_run_id, board_count, published_after
                FROM daily_import_runs
                WHERE status = 'running'
                  AND ats_scope = %s
                  AND (
                      lease_expires_at IS NULL
                      OR lease_expires_at < now()
                  )
                ORDER BY started_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (canonical_scope,),
            ).fetchone()
        else:
            existing = conn.execute(
                """
                SELECT import_run_id, board_count, published_after
                FROM daily_import_runs
                WHERE status = 'running'
                  AND ats_scope = %s
                  AND published_after = %s
                ORDER BY started_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (canonical_scope, published_after),
            ).fetchone()
        resumed = existing is not None
        if existing:
            import_run_id, board_count, effective_published_after = existing
            conn.execute(
                """
                UPDATE daily_import_runs
                SET updated_at = now(),
                    heartbeat_at = now(),
                    lease_expires_at = now() + make_interval(secs => %s),
                    worker_id = %s
                WHERE import_run_id = %s
                """,
                (lease_seconds, worker_id, import_run_id),
            )
        else:
            board_rows = conn.execute(
                """
                SELECT board_id, ats, slug
                FROM job_boards
                WHERE ats = ANY(%s)
                  AND active IS TRUE
                  AND closed_at IS NULL
                ORDER BY ats, slug
                """,
                (canonical_scope,),
            ).fetchall()
            board_count = len(board_rows)
            import_run_id = conn.execute(
                """
                INSERT INTO daily_import_runs (
                    ats_scope, published_after, board_count,
                    heartbeat_at, lease_expires_at, worker_id
                ) VALUES (
                    %s, %s, %s, now(),
                    now() + make_interval(secs => %s), %s
                )
                RETURNING import_run_id
                """,
                (
                    canonical_scope,
                    published_after,
                    board_count,
                    lease_seconds,
                    worker_id,
                ),
            ).fetchone()[0]
            effective_published_after = published_after
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO daily_import_run_boards (
                        import_run_id, board_id, position
                    ) VALUES (%s, %s, %s)
                    """,
                    [
                        (import_run_id, board_id, position)
                        for position, (board_id, _, _) in enumerate(board_rows, 1)
                    ],
                )
    pending = conn.execute(
        """
        SELECT rb.position, rb.board_id, b.ats, b.slug
        FROM daily_import_run_boards AS rb
        JOIN job_boards AS b ON b.board_id = rb.board_id
        WHERE rb.import_run_id = %s
          AND rb.status IN ('pending', 'failed')
          AND (
              rb.retry_after IS NULL
              OR rb.retry_after <= now()
          )
        ORDER BY rb.position
        """,
        (import_run_id,),
    ).fetchall()
    conn.commit()
    return (
        import_run_id,
        pending,
        resumed,
        board_count,
        effective_published_after,
    )


def _record_daily_board_success(
    cur: psycopg.Cursor,
    import_run_id: int,
    board_id: int,
    *,
    fetched: int,
    inserted: int,
    updated: int,
    status: str | None = None,
    worker_id: str | None = None,
) -> None:
    cur.execute(
        """
        UPDATE daily_import_run_boards
        SET status = %s,
            attempts = attempts + 1,
            jobs_fetched = %s,
            jobs_inserted = %s,
            jobs_updated = %s,
            error_type = NULL,
            error_message = NULL,
            retry_after = NULL,
            completed_at = now()
        WHERE import_run_id = %s AND board_id = %s
        """,
        (
            status or ("completed" if fetched else "empty"),
            fetched,
            inserted,
            updated,
            import_run_id,
            board_id,
        ),
    )
    cur.execute(
        """
        UPDATE daily_import_runs
        SET updated_at = now(),
            heartbeat_at = now(),
            lease_expires_at = now() + make_interval(secs => %s),
            worker_id = COALESCE(%s, worker_id)
        WHERE import_run_id = %s
        """,
        (DAILY_IMPORT_LEASE_SECONDS, worker_id, import_run_id),
    )


def _deactivate_workable_board(
    cur: psycopg.Cursor,
    slug: str,
) -> int:
    """Remove an empty Workable account from the active PostgreSQL registry."""
    cur.execute(
        """
        UPDATE job_boards
        SET active = FALSE
        WHERE ats = 'workable'
          AND slug = %s
          AND active IS TRUE
        """,
        (slug,),
    )
    return cur.rowcount


def _record_daily_board_failure(
    conn: psycopg.Connection,
    import_run_id: int,
    board_id: int,
    exc: BaseException,
    *,
    worker_id: str | None = None,
    automatic_retry: bool = False,
) -> None:
    with conn.transaction():
        conn.execute(
            """
            UPDATE daily_import_run_boards
            SET status = 'failed',
                attempts = attempts + 1,
                error_type = %s,
                error_message = %s,
                retry_after = CASE
                    WHEN %s THEN now() + make_interval(
                        secs => LEAST(
                            3600,
                            60 * power(2, LEAST(attempts + 1, 6))
                        )
                    )
                    ELSE NULL
                END,
                completed_at = now()
            WHERE import_run_id = %s AND board_id = %s
            """,
            (
                type(exc).__name__,
                str(exc)[:4000],
                automatic_retry,
                import_run_id,
                board_id,
            ),
        )
        conn.execute(
            """
            UPDATE daily_import_runs
            SET updated_at = now(),
                heartbeat_at = now(),
                lease_expires_at = now() + make_interval(secs => %s),
                worker_id = COALESCE(%s, worker_id)
            WHERE import_run_id = %s
            """,
            (DAILY_IMPORT_LEASE_SECONDS, worker_id, import_run_id),
        )


def _daily_import_counts(
    conn: psycopg.Connection,
    import_run_id: int,
) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            count(*) FILTER (WHERE status = 'pending'),
            count(*) FILTER (WHERE status = 'failed'),
            count(*) FILTER (WHERE status = 'completed'),
            count(*) FILTER (WHERE status = 'empty')
        FROM daily_import_run_boards
        WHERE import_run_id = %s
        """,
        (import_run_id,),
    ).fetchone()
    conn.commit()
    return dict(zip(("pending", "failed", "completed", "empty"), row))


def _daily_failures_allow_downstream(failed_boards: int, board_count: int) -> bool:
    """Allow downstream work when the board snapshot is at least 98% successful."""
    return failed_boards == 0 or (
        board_count > 0
        and (board_count - failed_boards) * 100 >= board_count * DAILY_MIN_SUCCESS_PERCENT
    )


def _finish_daily_import_run(
    dsn: str,
    import_run_id: int,
    *,
    failed_boards: int,
    downstream_failed: bool,
) -> None:
    status = (
        "completed_with_errors"
        if failed_boards or downstream_failed
        else "completed"
    )
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE daily_import_runs
                SET status = %s,
                    failed_board_count = %s,
                    updated_at = now(),
                    heartbeat_at = now(),
                    lease_expires_at = NULL,
                    worker_id = NULL,
                    completed_at = now()
                WHERE import_run_id = %s
                """,
                (status, failed_boards, import_run_id),
            )


def _daily_workday_limit(
    queues: dict[str, deque[tuple[int, int, str, str]]],
    active_by_ats: dict[str, int],
) -> int:
    """Promote Workday only after included non-Workday work has finished."""
    if not any(ats != "workday" for ats in queues):
        return 1
    other_queued = any(
        queue for ats, queue in queues.items() if ats != "workday"
    )
    other_active = any(
        count for ats, count in active_by_ats.items() if ats != "workday"
    )
    return 1 if other_queued or other_active else 2


def _daily_board_limit(
    ats: str,
    queues: dict[str, deque[tuple[int, int, str, str]]] | None = None,
    active_by_ats: dict[str, int] | None = None,
) -> int:
    if ats == "workday" and queues is not None and active_by_ats is not None:
        return _daily_workday_limit(queues, active_by_ats)
    return DAILY_ATS_CONCURRENCY.get(ats, DAILY_BOARD_WORKERS)


def _heartbeat_daily_import(
    conn: psycopg.Connection,
    import_run_id: int,
    worker_id: str,
) -> None:
    with conn.transaction():
        conn.execute(
            """
            UPDATE daily_import_runs
            SET updated_at = now(),
                heartbeat_at = now(),
                lease_expires_at = now() + make_interval(secs => %s),
                worker_id = %s
            WHERE import_run_id = %s
              AND status = 'running'
            """,
            (DAILY_IMPORT_LEASE_SECONDS, worker_id, import_run_id),
        )


def _import_daily_board(
    dsn: str,
    item: tuple[int, int, str, str],
    daily_board_count: int,
    import_run_id: int,
    published_after: datetime | None,
    greenhouse_content: bool,
    worker_id: str,
    automatic_retry: bool,
) -> dict[str, int]:
    """Fetch and commit one daily board using a connection owned by this worker."""
    position, tracked_board_id, ats, slug = item
    board_id: int | None = tracked_board_id or None
    connection: psycopg.Connection | None = None
    board_worker_id = f"{worker_id}:board-{tracked_board_id}"

    try:
        connection = psycopg.connect(dsn)
        print(
            f"{position}/{daily_board_count} {ats}/{slug}: fetching...",
            flush=True,
        )
        board_id, stored_etag, cached_published_after = _board_fetch_state(
            connection, ats, slug
        )
        conditional_etag = (
            stored_etag
            if stored_etag
            and _etag_covers(cached_published_after, published_after)
            else None
        )
        fetch_meta: dict[str, Any] = {}
        cached_workday_jobs = (
            _cached_workday_jobs(connection, board_id, slug)
            if ats == "workday"
            else None
        )
        # End the implicit read transaction before fetching and before the write
        # transaction. This keeps the board commit independently visible.
        connection.commit()
        rows, skipped = _fetch_normalized(
            ats,
            slug,
            published_after,
            greenhouse_content=greenhouse_content,
            etag=conditional_etag,
            meta=fetch_meta,
            cached_workday_jobs=cached_workday_jobs,
            detail_concurrency=DAILY_DETAIL_CONCURRENCY,
        )
        empty_workable_board = (
            ats == "workable"
            and fetch_meta.get("workable_source_job_count") == 0
        )
        with connection.transaction():
            with connection.cursor() as cur:
                seen_at = datetime.now(timezone.utc)
                board_id = _ensure_board(cur, ats, slug, seen_at)
                company = _board_company_name(cur, board_id)
                existing = _count_existing(cur, rows)
                new = len(rows) - existing
                _, closed = _upsert_board_jobs(
                    cur,
                    board_id,
                    company,
                    rows,
                    seen_at,
                    close_missing=published_after is None,
                )
                _save_board_etag(
                    cur,
                    board_id,
                    fetch_meta.get("etag"),
                    seen_at,
                    published_after,
                )
                deactivated = (
                    _deactivate_workable_board(cur, slug)
                    if empty_workable_board
                    else 0
                )
                _record_daily_board_success(
                    cur,
                    import_run_id,
                    board_id,
                    fetched=len(rows),
                    inserted=new,
                    updated=existing,
                    worker_id=board_worker_id,
                )
        if empty_workable_board:
            cache_removed = job_boards.remove_workable_board_from_cache(slug)
            print(
                f"{position}/{daily_board_count} workable/{slug}: "
                "no current jobs; deactivated PostgreSQL board "
                f"({deactivated}) and "
                f"{'removed it from boards.json' if cache_removed else 'it was not in boards.json'}; "
                "historical jobs retained",
                flush=True,
            )
        print(
            f"{position}/{daily_board_count} {ats}/{slug}: "
            f"{len(rows)} jobs ({new} new, {existing} updated, "
            f"{skipped} before cutoff, {closed} closed)",
            flush=True,
        )
        return {
            "jobs": len(rows),
            "new": new,
            "updated": existing,
            "closed": closed,
            "unchanged": 0,
            "failed": 0,
            "pruned": 1 if empty_workable_board else 0,
        }
    except job_boards.NotModified:
        if connection is not None:
            connection.rollback()
            try:
                with connection.transaction():
                    with connection.cursor() as cur:
                        seen_at = datetime.now(timezone.utc)
                        if board_id is not None:
                            _mark_board_unchanged(
                                cur,
                                board_id,
                                seen_at,
                                published_after,
                            )
                            _record_daily_board_success(
                                cur,
                                import_run_id,
                                board_id,
                                fetched=0,
                                inserted=0,
                                updated=0,
                                status="completed",
                                worker_id=board_worker_id,
                            )
            except Exception:
                raise
        print(
            f"{position}/{daily_board_count} {ats}/{slug}: unchanged (304, ETag)",
            flush=True,
        )
        return {
            "jobs": 0,
            "new": 0,
            "updated": 0,
            "closed": 0,
            "unchanged": 1,
            "failed": 0,
            "pruned": 0,
        }
    except job_boards.NotFound as exc:
        if connection is not None:
            connection.rollback()
        if connection is not None:
            _record_daily_board_failure(
                connection,
                import_run_id,
                board_id or tracked_board_id,
                exc,
                worker_id=board_worker_id,
                automatic_retry=automatic_retry,
            )
        print(
            f"{position}/{daily_board_count} {ats}/{slug}: 404",
            file=sys.stderr,
            flush=True,
        )
        return {
            "jobs": 0,
            "new": 0,
            "updated": 0,
            "closed": 0,
            "unchanged": 0,
            "failed": 1,
            "pruned": 0,
        }
    except Exception as exc:
        if connection is not None:
            connection.rollback()
            try:
                _record_daily_board_failure(
                    connection,
                    import_run_id,
                    board_id or tracked_board_id,
                    exc,
                    worker_id=board_worker_id,
                    automatic_retry=automatic_retry,
                )
            except Exception as record_exc:
                print(
                    f"{position}/{daily_board_count} {ats}/{slug}: "
                    f"could not record failure ({record_exc})",
                    file=sys.stderr,
                    flush=True,
                )
        print(
            f"{position}/{daily_board_count} {ats}/{slug}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return {
            "jobs": 0,
            "new": 0,
            "updated": 0,
            "closed": 0,
            "unchanged": 0,
            "failed": 1,
            "pruned": 0,
        }
    finally:
        if connection is not None:
            connection.close()


def _run_daily_boards_parallel(
    dsn: str,
    daily_scan_items: list[tuple[int, int, str, str]],
    daily_board_count: int,
    import_run_id: int,
    published_after: datetime | None,
    greenhouse_content: bool,
    worker_id: str,
    automatic_retry: bool,
) -> dict[str, int]:
    """Run the immutable daily snapshot with bounded, fair board concurrency."""
    queues: dict[str, deque[tuple[int, int, str, str]]] = {}
    ats_order: list[str] = []
    for item in daily_scan_items:
        ats = item[2]
        if ats not in queues:
            queues[ats] = deque()
            ats_order.append(ats)
        queues[ats].append(item)

    active: dict[Any, tuple[str, tuple[int, int, str, str]]] = {}
    active_by_ats: dict[str, int] = {ats: 0 for ats in ats_order}
    round_robin_index = 0
    last_workday_limit: int | None = None
    stats = {
        "jobs": 0,
        "new": 0,
        "updated": 0,
        "closed": 0,
        "unchanged": 0,
        "failed": 0,
        "pruned": 0,
    }
    print(
        f"Daily import parallelism: {DAILY_BOARD_WORKERS} total board workers; "
        f"Workday=1, SmartRecruiters=1; detail concurrency={DAILY_DETAIL_CONCURRENCY}",
        flush=True,
    )

    with psycopg.connect(dsn) as heartbeat_connection:
        with ThreadPoolExecutor(max_workers=DAILY_BOARD_WORKERS) as pool:
            while active or any(queues[ats] for ats in ats_order):
                while len(active) < DAILY_BOARD_WORKERS and ats_order:
                    selected: tuple[str, tuple[int, int, str, str]] | None = None
                    for offset in range(len(ats_order)):
                        index = (round_robin_index + offset) % len(ats_order)
                        ats = ats_order[index]
                        workday_limit = _daily_workday_limit(
                            queues,
                            active_by_ats,
                        )
                        if workday_limit != last_workday_limit:
                            print(
                                f"Daily Workday board limit: {workday_limit} "
                                "(promoted after non-Workday work drains)"
                                if workday_limit == 2
                                else "Daily Workday board limit: 1",
                                flush=True,
                            )
                            last_workday_limit = workday_limit
                        if (
                            queues[ats]
                            and active_by_ats[ats]
                            < (
                                workday_limit
                                if ats == "workday"
                                else _daily_board_limit(ats)
                            )
                        ):
                            selected = (ats, queues[ats].popleft())
                            round_robin_index = (index + 1) % len(ats_order)
                            break
                    if selected is None:
                        break
                    ats, item = selected
                    active_by_ats[ats] += 1
                    future = pool.submit(
                        _import_daily_board,
                        dsn,
                        item,
                        daily_board_count,
                        import_run_id,
                        published_after,
                        greenhouse_content,
                        worker_id,
                        automatic_retry,
                    )
                    active[future] = (ats, item)

                if not active:
                    # This should only be possible if the scheduler configuration is
                    # invalid; avoid spinning forever if it ever happens.
                    raise RuntimeError(
                        "daily board scheduler could not dispatch pending work"
                    )

                completed, _ = wait(
                    active,
                    timeout=30,
                    return_when=FIRST_COMPLETED,
                )
                if not completed:
                    _heartbeat_daily_import(
                        heartbeat_connection,
                        import_run_id,
                        worker_id,
                    )
                    continue
                for future in completed:
                    ats, item = active.pop(future)
                    active_by_ats[ats] -= 1
                    try:
                        result = future.result()
                    except Exception as exc:
                        position, _, failed_ats, slug = item
                        print(
                            f"{position}/{daily_board_count} {failed_ats}/{slug}: "
                            f"worker failure {type(exc).__name__}: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        result = {
                            "jobs": 0,
                            "new": 0,
                            "updated": 0,
                            "closed": 0,
                            "unchanged": 0,
                            "failed": 1,
                            "pruned": 0,
                        }
                    for key in stats:
                        stats[key] += result[key]
    return stats


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
    etag: str | None = None,
    meta: dict[str, Any] | None = None,
    cached_workday_jobs: dict[str, dict[str, Any]] | None = None,
    detail_concurrency: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    source = job_boards.SOURCES[ats]
    if source.get("fetch_jobs"):
        if ats == "workday":
            raw_jobs = source["fetch_jobs"](
                slug,
                published_after,
                cached_workday_jobs,
                detail_concurrency,
            )
        elif ats == "smartrecruiters" and detail_concurrency is not None:
            raw_jobs = source["fetch_jobs"](
                slug,
                published_after,
                detail_concurrency,
            )
        else:
            raw_jobs = source["fetch_jobs"](slug, published_after)
    else:
        payload = json.loads(
            job_boards.fetch(
                job_boards.board_url(
                    ats,
                    slug,
                    want_content=greenhouse_content and ats == "greenhouse",
                ),
                timeout=30,
                etag=etag,
                meta=meta,
            )
        )
        raw_jobs = source["jobs"](payload)
    if not isinstance(raw_jobs, list):
        raise ValueError(f"{ats}/{slug}: response has no jobs array")
    if ats == "workable" and meta is not None:
        meta["workable_source_job_count"] = getattr(
            raw_jobs,
            "source_job_count",
            None,
        )

    normalized_rows: list[dict[str, Any]] = []
    skipped = 0
    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            continue
        normalized = source["normalize"](raw_job)
        if normalized is None:
            continue
        if ats in {
            "smartrecruiters",
            "workday",
            "recruitee",
            "teamtailor",
            "workable",
        }:
            normalized["id"] = f"{slug}:{normalized['id']}"
        if ats == "workable" and not normalized.get("jobUrl"):
            normalized["jobUrl"] = job_boards.workable_job_url(
                slug,
                str(raw_job.get("shortcode") or normalized["id"]),
            )
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
            "address": normalized.get("address"),
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


def _board_fetch_state(
    conn: psycopg.Connection,
    ats: str,
    slug: str,
) -> tuple[int | None, str | None, datetime | None]:
    """Return the board id, ETag, and cutoff covered by that ETag."""
    row = conn.execute(
        """
        SELECT board_id, etag, etag_published_after
        FROM job_boards
        WHERE ats = %s AND slug = %s
        """,
        (ats, slug),
    ).fetchone()
    return row if row else (None, None, None)


def _cached_workday_jobs(
    conn: psycopg.Connection,
    board_id: int | None,
    slug: str,
) -> dict[str, dict[str, Any]]:
    """Return persisted Workday detail evidence keyed by raw requisition ID."""
    if board_id is None:
        return {}
    prefix = f"{slug}:"
    rows = conn.execute(
        """
        SELECT external_id, published_at, description_text, location_raw,
               workplace_type, employment_type
        FROM jobs
        WHERE board_id = %s
        """,
        (board_id,),
    ).fetchall()
    cached: dict[str, dict[str, Any]] = {}
    for row in rows:
        external_id = str(row[0] or "")
        if not external_id.startswith(prefix):
            continue
        cached[external_id[len(prefix):]] = {
            "published_at": row[1],
            "description_text": row[2],
            "location_raw": row[3],
            "workplace_type": row[4],
            "employment_type": row[5],
        }
    return cached


def _etag_covers(
    cached_published_after: datetime | None,
    requested_published_after: datetime | None,
) -> bool:
    """Whether a stored response can safely answer this filtered scan.

    A response fetched without a cutoff covers every later cutoff. A response
    fetched with a cutoff only covers the same or a newer cutoff, since a
    request for older jobs could require records that were never persisted.
    """
    if cached_published_after is None:
        return True
    return (
        requested_published_after is not None
        and requested_published_after >= cached_published_after
    )


def _save_board_etag(
    cur: psycopg.Cursor,
    board_id: int,
    etag: str | None,
    seen_at: datetime,
    published_after: datetime | None,
) -> None:
    cur.execute(
        """
        UPDATE job_boards
        SET etag = %s,
            etag_seen_at = %s,
            etag_published_after = %s
        WHERE board_id = %s
        """,
        (etag, seen_at, published_after, board_id),
    )


def _mark_board_unchanged(
    cur: psycopg.Cursor,
    board_id: int,
    seen_at: datetime,
    requested_published_after: datetime | None,
) -> None:
    """Refresh board health after a 304 without touching job last_seen values."""
    cur.execute(
        """
        UPDATE job_boards
        SET last_seen = %s,
            closed_at = NULL,
            etag_seen_at = %s,
            etag_published_after = %s
        WHERE board_id = %s
        """,
        (seen_at, seen_at, requested_published_after, board_id),
    )


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


def _sync_board_registry(
    cur: psycopg.Cursor,
    specs: list[tuple[str, str]],
    seen_at: datetime,
) -> int:
    """Register newly discovered boards without changing existing board state."""
    added = 0
    for ats, slug in specs:
        cur.execute(
            "SELECT board_id FROM job_boards WHERE ats = %s AND slug = %s",
            (ats, slug),
        )
        if cur.fetchone():
            continue
        _ensure_board(cur, ats, slug, seen_at)
        added += 1
    return added


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
                address, published_at, source_updated_at, job_url, description_text,
                first_seen, last_seen, updated_at
            ) VALUES (
                %(board_id)s, %(company)s, %(ats)s, %(external_id)s, %(title)s,
                %(department)s, %(team)s, %(employment_type)s,
                %(location_raw)s, %(is_remote)s, %(workplace_type)s,
                %(address)s,
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
                address = EXCLUDED.address,
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
            {
                **row,
                "address": (
                    Jsonb(row["address"])
                    if row.get("address") is not None
                    else None
                ),
                "board_id": board_id,
                "company": company,
                "seen_at": seen_at,
            },
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


def _normalized_search_text(value: Any) -> str:
    """Fold accents and punctuation for stable, human-readable preference checks."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
    return re.sub(r"\s+", " ", text).strip()


def _preference_values(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        values = value
    else:
        values = [value]
    return [
        normalized
        for item in values
        if (normalized := _normalized_search_text(item))
    ]


def _contains_preference(text: Any, preferences: Any) -> bool:
    normalized_text = _normalized_search_text(text)
    return bool(normalized_text) and any(
        preference in normalized_text
        for preference in _preference_values(preferences)
    )


def _text_similarity(value: Any, text: Any) -> float:
    """Return a conservative deterministic phrase/token similarity in [0, 1]."""
    normalized_value = _normalized_match_text(value)
    normalized_text = _normalized_match_text(text)
    if not normalized_value or not normalized_text:
        return 0.0
    padded_text = f" {normalized_text} "
    if f" {normalized_value} " in padded_text:
        return 1.0
    value_tokens = set(normalized_value.split())
    text_tokens = set(normalized_text.split())
    if not value_tokens:
        return 0.0
    return len(value_tokens & text_tokens) / len(value_tokens)


def _feedback_adjustment(
    job: dict[str, Any],
    examples: list[tuple[int, str, str, int]],
) -> int:
    """Apply a bounded title-similarity signal from prior user decisions."""
    strongest: dict[str, float] = {}
    for prior_job_id, prior_title, status, weight in examples:
        if prior_job_id == job["job_id"]:
            continue
        similarity = _text_similarity(job.get("title"), prior_title)
        if similarity < 0.35:
            continue
        signed = float(weight) * similarity * 2
        if status not in strongest or abs(signed) > abs(strongest[status]):
            strongest[status] = signed
    return max(-10, min(10, round(sum(strongest.values()))))


def _normalized_match_text(value: Any) -> str:
    normalized = _normalized_search_text(value)
    return " ".join(
        _MATCH_WORD_ALIASES.get(token, token)
        for token in normalized.split()
    )


def _best_text_similarity(values: Any, text: Any) -> float:
    return max(
        (_text_similarity(value, text) for value in _preference_values(values)),
        default=0.0,
    )


def _profile_items(user: dict[str, Any], key: str) -> list[dict[str, Any]]:
    profile = user.get("profile_json")
    if not isinstance(profile, dict):
        return []
    values = profile.get(key)
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, dict)]


def _profile_item_weight(item: dict[str, Any], field: str = "strength") -> float:
    weight = _PROFILE_STRENGTH_WEIGHTS.get(str(item.get(field) or "").casefold(), 0.5)
    recency = _PROFILE_RECENCY_WEIGHTS.get(
        str(item.get("recency") or "unknown").casefold(),
        1.0,
    )
    return weight * recency


def _weighted_profile_similarity(
    items: list[dict[str, Any]],
    value_key: str,
    text: Any,
    *,
    weight_field: str = "strength",
) -> float:
    scores = []
    for item in items:
        value = item.get(value_key)
        similarity = _text_similarity(value, text)
        if similarity:
            scores.append(
                (
                    similarity
                    * (
                        _PROFILE_PROFICIENCY_WEIGHTS.get(
                            str(item.get(weight_field) or "").casefold(),
                            _profile_item_weight(item, weight_field),
                        )
                        if weight_field == "proficiency"
                        else _profile_item_weight(item, weight_field)
                    ),
                    _PROFILE_PROFICIENCY_WEIGHTS.get(
                        str(item.get(weight_field) or "").casefold(),
                        0.5,
                    )
                    if weight_field == "proficiency"
                    else _PROFILE_STRENGTH_WEIGHTS.get(
                        str(item.get(weight_field) or "").casefold(),
                        0.5,
                    ),
                )
            )
    if not scores:
        return 0.0
    top_scores = sorted(scores, reverse=True)[:3]
    return sum(score for score, _ in top_scores) / sum(
        weight for _, weight in top_scores
    )


def _experience_years_weight(item: dict[str, Any]) -> float:
    years = item.get("years")
    if isinstance(years, (int, float)) and not isinstance(years, bool):
        return min(1.0, 0.4 + (max(0.0, float(years)) * 0.06))
    return 0.75


def _profile_role_similarity(
    user: dict[str, Any],
    role_text: str,
) -> float:
    scores = []
    for item in _profile_items(user, "experience_areas"):
        similarity = _text_similarity(item.get("area"), role_text)
        if similarity:
            scores.append(
                similarity
                * _profile_item_weight(item)
                * _experience_years_weight(item)
            )
    return max(scores, default=0.0)


def _role_fit(user: dict[str, Any], job: dict[str, Any], role_text: str) -> float:
    target_values = _preference_values(user.get("target_roles"))
    target_score = _best_text_similarity(target_values, role_text)
    profile_items = _profile_items(user, "experience_areas")
    profile_score = _profile_role_similarity(user, role_text)
    if not target_values and not profile_items:
        return 1.0
    if target_values and profile_items:
        # Explicit target roles establish intent, while demonstrated functional
        # depth breaks ties between equally preferred role titles.
        return (target_score * 0.4) + (profile_score * 0.6)
    return target_score or profile_score


def _industry_fit(
    user: dict[str, Any],
    job: dict[str, Any],
    metadata_text: str,
    full_text: str,
) -> float:
    target_values = user.get("target_industries")
    target_metadata = _best_text_similarity(target_values, metadata_text)
    target_text = _best_text_similarity(target_values, full_text)
    profile_metadata = _weighted_profile_similarity(
        _profile_items(user, "industries"),
        "industry",
        metadata_text,
    )
    profile_text = _weighted_profile_similarity(
        _profile_items(user, "industries"),
        "industry",
        full_text,
    )
    has_industry_evidence = bool(
        _preference_values(target_values)
        or _profile_items(user, "industries")
    )
    if not has_industry_evidence:
        return 1.0
    # Explicit company metadata is strongest. Text-only matches are useful but
    # discounted because a job description can mention an industry incidentally.
    return max(
        target_metadata,
        profile_metadata,
        target_text * 0.8,
        profile_text * 0.8,
    )


def _capability_fit(user: dict[str, Any], capability_text: str) -> float:
    profile = user.get("profile_json")
    if not isinstance(profile, dict):
        return 1.0
    signals: list[tuple[str, float]] = []
    for item in _profile_items(user, "skills"):
        value = item.get("skill")
        if value:
            signals.append(
                (
                    str(value),
                    _PROFILE_STRENGTH_WEIGHTS.get(
                        str(item.get("strength") or "").casefold(),
                        0.5,
                    ),
                )
            )
    for item in _profile_items(user, "technologies_tools_methodologies"):
        value = item.get("name")
        if value:
            signals.append(
                (
                    str(value),
                    _PROFILE_PROFICIENCY_WEIGHTS.get(
                        str(item.get("proficiency") or "").casefold(),
                        0.5,
                    ),
                )
            )
    for item in _profile_items(user, "transferable_capabilities"):
        value = item.get("capability")
        if value:
            signals.append((str(value), 0.7))
    if not signals:
        return 1.0
    matches = [
        (_text_similarity(value, capability_text), weight)
        for value, weight in signals
        if _text_similarity(value, capability_text)
    ]
    if not matches:
        return 0.0
    top_matches = sorted(
        matches,
        key=lambda pair: pair[0] * pair[1],
        reverse=True,
    )[:3]
    return sum(score * weight for score, weight in top_matches) / sum(
        weight for _, weight in top_matches
    )


def _seniority_rank(value: Any) -> int | None:
    normalized = _normalized_search_text(value)
    if not normalized:
        return None
    padded = f" {normalized} "
    for term, rank in _SENIORITY_RANKS:
        if f" {term} " in padded:
            return rank
    return None


def _seniority_fit(user: dict[str, Any], role_text: str) -> float:
    profile = user.get("profile_json")
    if not isinstance(profile, dict):
        return 1.0
    experience_items = _profile_items(user, "experience_areas")
    best_experience = max(
        experience_items,
        key=lambda item: (
            _text_similarity(item.get("area"), role_text),
            _profile_role_similarity(
                {"profile_json": {"experience_areas": [item]}},
                role_text,
            ),
        ),
        default=None,
    )
    if best_experience and _text_similarity(best_experience.get("area"), role_text):
        profile_rank = _seniority_rank(best_experience.get("seniority"))
    else:
        seniority = profile.get("seniority")
        profile_rank = _seniority_rank(
            seniority.get("level") if isinstance(seniority, dict) else None
        )
    if profile_rank is None:
        return 1.0
    job_rank = _seniority_rank(role_text)
    if job_rank is None:
        level_fit = 0.65
    else:
        distance = abs(profile_rank - job_rank)
        level_fit = {0: 1.0, 1: 0.8, 2: 0.55}.get(distance, 0.3)

    leadership_items = _profile_items(user, "leadership_and_responsibility")
    has_profile_leadership = bool(leadership_items)
    has_job_leadership = any(
        term in f" {_normalized_search_text(role_text)} "
        for term in ("manager", "lead", "director", "head", "supervisor")
    )
    leadership_fit = (
        1.0
        if has_profile_leadership == has_job_leadership
        else 0.6
    )
    return (level_fit * 0.75) + (leadership_fit * 0.25)


def _workplace_kind(job: dict[str, Any]) -> str:
    workplace = _normalized_search_text(job.get("workplace_type"))
    location = _normalized_search_text(job.get("location_raw"))
    if "hybrid" in workplace or "hybrid" in location:
        return "hybrid"
    if (
        "onsite" in workplace
        or "on site" in workplace
        or "on site" in location
    ):
        return "onsite"
    if job.get("is_remote") or "remote" in workplace or "remote" in location:
        return "remote"
    return "onsite"


_LOCATION_COUNTRY_ALIASES = {
    "us": {"us", "usa", "united states", "united states of america"},
    "ca": {"ca", "canada"},
    "gb": {"gb", "uk", "united kingdom", "great britain", "england"},
    "es": {"es", "spain"},
    "pt": {"pt", "portugal"},
    "de": {"de", "germany"},
    "fr": {"fr", "france"},
    "it": {"it", "italy"},
    "nl": {"nl", "netherlands", "holland"},
    "pl": {"pl", "poland"},
    "ie": {"ie", "ireland"},
    "se": {"se", "sweden"},
    "no": {"no", "norway"},
    "dk": {"dk", "denmark"},
    "fi": {"fi", "finland"},
    "at": {"at", "austria"},
    "be": {"be", "belgium"},
    "ch": {"ch", "switzerland"},
    "cz": {"cz", "czechia", "czech republic"},
    "ro": {"ro", "romania"},
    "bg": {"bg", "bulgaria"},
    "gr": {"gr", "greece"},
    "hu": {"hu", "hungary"},
    "ee": {"ee", "estonia"},
    "lv": {"lv", "latvia"},
    "lt": {"lt", "lithuania"},
    "hr": {"hr", "croatia"},
    "si": {"si", "slovenia"},
    "sk": {"sk", "slovakia"},
    "cy": {"cy", "cyprus"},
    "mt": {"mt", "malta"},
    "lu": {"lu", "luxembourg"},
    "is": {"is", "iceland"},
    "al": {"al", "albania"},
    "ba": {"ba", "bosnia and herzegovina"},
    "me": {"me", "montenegro"},
    "mk": {"mk", "north macedonia"},
    "rs": {"rs", "serbia"},
    "ua": {"ua", "ukraine"},
    "md": {"md", "moldova"},
    "ge": {"ge", "georgia"},
    "tr": {"tr", "turkey"},
    "au": {"au", "australia"},
    "cn": {"cn", "china"},
    "hk": {"hk", "hong kong"},
    "in": {"in", "india"},
    "jp": {"jp", "japan"},
    "sg": {"sg", "singapore"},
}
_LOCATION_CITY_COUNTRIES = {
    "estepona": "es",
    "marbella": "es",
    "madrid": "es",
    "malaga": "es",
    "barcelona": "es",
    "valencia": "es",
    "seville": "es",
    "sevilla": "es",
    "lisbon": "pt",
    "porto": "pt",
    "faro": "pt",
    "gibraltar": "gi",
    "paris": "fr",
    "berlin": "de",
    "hamburg": "de",
    "munich": "de",
    "istanbul": "tr",
    "budapest": "hu",
    "london": "gb",
    "belfast": "gb",
    "krakow": "pl",
    "warsaw": "pl",
    "stockholm": "se",
    "toronto": "ca",
    "san francisco": "us",
    "new york": "us",
    "boston": "us",
    "sydney": "au",
}
_EUROPE_COUNTRY_GROUPS = {
    "al", "at", "ba", "be", "bg", "ch", "cy", "cz", "de", "dk", "ee",
    "es", "fi", "fr", "gb", "gr", "hr", "hu", "ie", "is", "it", "lt",
    "lu", "lv", "me", "mk", "mt", "nl", "no", "pl", "pt", "ro", "rs",
    "se", "si", "sk", "ua",
}
_EUROPE_REGION_ALIASES = {"europe", "eu", "eea", "emea"}
_REMOTE_TIMEZONE_TERMS = {
    "eastern time",
    "central time",
    "mountain time",
    "pacific time",
    "eastern timezone",
    "central timezone",
    "mountain timezone",
    "pacific timezone",
}
_DESCRIPTION_LOCATION_TRIGGERS = (
    "based",
    "located",
    "reside",
    "resident",
    "work from",
    "working from",
    "hire in",
    "hiring in",
    "must be",
    "required to",
    "work authorization",
    "authorized to work",
    "time zone",
    "timezone",
)


def _phrase_in_text(phrase: Any, text: str) -> bool:
    normalized = _normalized_search_text(phrase)
    return bool(normalized) and f" {normalized} " in f" {text} "


def _address_text(value: Any) -> str:
    """Flatten JSONB address evidence without depending on one ATS shape."""
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            item_text = _address_text(item)
            if str(key).casefold() in {"country", "addresscountry"}:
                parts.append(f"country {item_text}")
            else:
                parts.append(item_text)
        return " ".join(parts)
    if isinstance(value, (list, tuple)):
        return " ".join(_address_text(item) for item in value)
    return _normalized_search_text(value)


def _description_location_evidence(value: Any) -> str:
    """Keep only restrictive-looking description sentences, not EEO boilerplate."""
    text = str(value or "")
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    evidence = []
    geo_terms = set(_EUROPE_REGION_ALIASES)
    geo_terms.update(
        alias
        for aliases in _LOCATION_COUNTRY_ALIASES.values()
        for alias in aliases
    )
    geo_terms.update(_REMOTE_TIMEZONE_TERMS)
    for sentence in sentences:
        normalized = _normalized_search_text(sentence)
        if not normalized:
            continue
        has_geo = any(_phrase_in_text(term, normalized) for term in geo_terms)
        has_trigger = any(trigger in normalized for trigger in _DESCRIPTION_LOCATION_TRIGGERS)
        if has_geo and has_trigger:
            evidence.append(normalized)
    return " ".join(evidence)


def _location_evidence_text(job: dict[str, Any]) -> str:
    parts = [
        _normalized_search_text(job.get("location_raw")),
        _address_text(job.get("address")),
        _description_location_evidence(job.get("description_text")),
    ]
    return " ".join(part for part in parts if part)


def _location_groups(text: Any) -> set[str]:
    normalized = _normalized_search_text(text)
    padded = f" {normalized} "
    groups = set()
    for country, aliases in _LOCATION_COUNTRY_ALIASES.items():
        long_aliases = [alias for alias in aliases if len(alias) > 2]
        short_aliases = [alias for alias in aliases if len(alias) == 2]
        has_long_alias = any(_phrase_in_text(alias, normalized) for alias in long_aliases)
        has_country_code = any(
            _phrase_in_text(f"country {alias}", normalized)
            for alias in short_aliases
        )
        # US is common in human-readable labels ("Remote - US"); other
        # two-letter codes are only trusted in a country-labelled JSON field
        # or when the entire preference/evidence value is that code. This
        # prevents words such as "is" and "be" from becoming countries.
        has_us_label = country == "us" and _phrase_in_text("us", normalized)
        has_exact_code = normalized in short_aliases
        if has_long_alias or has_country_code or has_us_label or has_exact_code:
            groups.add(country)
    if any(_phrase_in_text(region, normalized) for region in _EUROPE_REGION_ALIASES):
        groups.add("europe")
    if any(_phrase_in_text(term, normalized) for term in _REMOTE_TIMEZONE_TERMS):
        groups.add("us")
    for city, country in _LOCATION_CITY_COUNTRIES.items():
        if _phrase_in_text(city, normalized):
            groups.add(country)
    if groups & _EUROPE_COUNTRY_GROUPS:
        groups.add("europe")
    return groups


def _concrete_location_preferences(user: dict[str, Any]) -> list[str]:
    """Return places where the user can actually be based for work."""
    values: list[str] = []
    for field in ("base_city", "base_country"):
        values.extend(_preference_values(user.get(field)))
    if user.get("willing_to_relocate"):
        for field in ("relocation_cities", "relocation_countries"):
            values.extend(_preference_values(user.get(field)))
    return values


def _city_location_preferences(user: dict[str, Any]) -> list[str]:
    """Return cities where the user can work from an office."""
    values: list[str] = []
    values.extend(_preference_values(user.get("base_city")))
    if user.get("willing_to_relocate"):
        values.extend(_preference_values(user.get("relocation_cities")))
    return values


def _location_city_groups(text: Any) -> set[str]:
    normalized = _normalized_search_text(text)
    return {
        country
        for city, country in _LOCATION_CITY_COUNTRIES.items()
        if _phrase_in_text(city, normalized)
    }


def _primary_address_text(value: Any) -> str:
    """Read only a provider's primary address when secondary offices exist."""
    if isinstance(value, dict) and isinstance(value.get("primary"), dict):
        return _address_text(value["primary"])
    return _address_text(value)


def _location_matches_city_scope(
    job: dict[str, Any],
    user: dict[str, Any],
) -> bool:
    """Require an onsite or hybrid role to name an allowed primary city."""
    allowed_cities = _city_location_preferences(user)
    if not allowed_cities:
        return False

    location_text = _normalized_search_text(job.get("location_raw"))
    if any(_phrase_in_text(city, location_text) for city in allowed_cities):
        return True
    if _location_city_groups(location_text):
        return False

    # Some normalized provider payloads keep the primary city only in address.
    # Do not inspect secondary offices when the primary location is already
    # concrete; an allowed secondary office must not override it.
    primary_address_text = _primary_address_text(job.get("address"))
    if any(_phrase_in_text(city, primary_address_text) for city in allowed_cities):
        return True
    return False


def _location_scope_groups(text: Any) -> tuple[set[str], bool, bool]:
    """Return location groups and distinguish Europe from broad EMEA scope."""
    normalized = _normalized_search_text(text)
    groups = _location_groups(normalized)
    has_europe_scope = any(
        _phrase_in_text(term, normalized)
        for term in (
            "europe",
            "eu",
            "eea",
            "european union",
            "european economic area",
        )
    )
    has_emea_scope = _phrase_in_text("emea", normalized)
    return groups, has_europe_scope, has_emea_scope


def _location_matches_concrete_scope(
    job: dict[str, Any],
    user: dict[str, Any],
) -> bool:
    """Require a role to permit work from a concrete user location.

    Country preferences are sufficient for remote roles, where Spain means
    remote work from anywhere in Spain. Office-based roles are stricter: their
    primary city must be one of the user's allowed cities.
    """
    concrete_preferences = _concrete_location_preferences(user)
    if not concrete_preferences:
        return True
    if _workplace_kind(job) in {"onsite", "hybrid"}:
        return _location_matches_city_scope(job, user)

    allowed_groups: set[str] = set()
    allowed_terms: set[str] = set()
    for preference in concrete_preferences:
        allowed_groups.update(_location_groups(preference))
        allowed_terms.add(preference)
    allowed_country_groups = allowed_groups & set(_LOCATION_COUNTRY_ALIASES)

    location_text = _normalized_search_text(job.get("location_raw"))
    address_text = _address_text(job.get("address"))
    location_groups, location_europe, location_emea = _location_scope_groups(
        location_text
    )
    explicit_location_countries = location_groups & set(_LOCATION_COUNTRY_ALIASES)
    if (
        any(_phrase_in_text(term, location_text) for term in allowed_terms)
        or location_groups & allowed_country_groups
    ):
        return True
    if explicit_location_countries:
        return False

    # A city/country in the job's primary location wins over unrelated
    # secondary offices in the provider address. A broad remote label is the
    # exception: its address may list the actual countries where hiring is
    # allowed.
    has_primary_location = bool(location_text)
    is_generic_remote = location_text in {
        "",
        "remote",
        "anywhere",
        "work from home",
        "work from anywhere",
    }
    if has_primary_location and not is_generic_remote:
        if _workplace_kind(job) == "remote" and location_europe and not location_emea:
            return True
        return False

    address_groups, address_europe, address_emea = _location_scope_groups(
        address_text
    )
    if address_groups & allowed_country_groups:
        return True
    if address_groups & set(_LOCATION_COUNTRY_ALIASES):
        return False
    if (
        _workplace_kind(job) == "remote"
        and (location_europe or address_europe)
        and not (location_emea or address_emea)
    ):
        return True

    # A generic "Remote" label may still be clarified by restrictive sentences
    # in an enriched description.
    description_text = _description_location_evidence(job.get("description_text"))
    description_groups, description_europe, description_emea = (
        _location_scope_groups(description_text)
    )
    if (
        any(_phrase_in_text(term, description_text) for term in allowed_terms)
        or description_groups & allowed_country_groups
    ):
        return True
    if description_groups & set(_LOCATION_COUNTRY_ALIASES):
        return False
    if (
        _workplace_kind(job) == "remote"
        and description_europe
        and not description_emea
    ):
        return True
    return (
        _workplace_kind(job) == "remote"
        and _generic_remote_allowed_for_user(user)
    )


def _generic_remote_allowed_for_user(user: dict[str, Any]) -> bool:
    """Unknown remote geography is conservative except for an explicit US preference."""
    return any(
        "us" in _location_groups(value)
        for value in _concrete_location_preferences(user)
    )


def _location_is_configured(user: dict[str, Any]) -> bool:
    return any(
        _preference_values(user.get(field))
        for field in (
            "base_city",
            "base_country",
            "relocation_cities",
            "relocation_countries",
        )
    )


def _location_matches(job: dict[str, Any], user: dict[str, Any]) -> bool:
    """Match structured and textual location evidence to a user's preferences."""
    if not _location_is_configured(user):
        return True
    return _location_matches_concrete_scope(job, user)


def _workplace_matches(job: dict[str, Any], user: dict[str, Any]) -> bool:
    kind = _workplace_kind(job)
    preference_field = {
        "remote": "remote_allowed",
        "onsite": "onsite_allowed",
        "hybrid": "hybrid_allowed",
    }[kind]
    configured = any(
        user.get(field) is not None
        for field in ("remote_allowed", "onsite_allowed", "hybrid_allowed")
    )
    return not configured or bool(user.get(preference_field))


def evaluate_job_match(
    user: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any] | None:
    """Return a deterministic preference score, or None for a hard exclusion.

    The score is 100 points: role 40, industry 25, profile capabilities 20, and
    seniority 15. Workplace and location are hard filters, not score components.
    Empty preference dimensions are treated as unconstrained and receive their
    full weight. All hard-filter-eligible jobs are returned regardless of score;
    ``min_match_score`` is retained as profile metadata for later analysis.
    Closed jobs are hard exclusions.
    The SQL candidate query also filters closed jobs so a matching run does not
    load them.
    """
    if job.get("closed_at") is not None:
        return None
    if not _workplace_matches(job, user):
        return None

    role_text = " ".join(
        str(job.get(field) or "")
        for field in ("title", "department", "team")
    )
    industry_text = " ".join(
        str(job.get(field) or "")
        for field in (
            "industry",
            "category",
            "company",
            "title",
            "department",
            "team",
            "description_text",
        )
    )
    metadata_text = " ".join(
        str(job.get(field) or "")
        for field in ("industry", "category")
    )
    location_configured = _location_is_configured(user)
    location_match = not location_configured or _location_matches(job, user)
    if not location_match:
        return None

    role_fit = _role_fit(user, job, role_text)
    industry_fit = _industry_fit(user, job, metadata_text, industry_text)
    profile = user.get("profile_json")
    if isinstance(profile, dict):
        capability_fit = _capability_fit(user, industry_text + " " + role_text)
        seniority_fit = _seniority_fit(user, role_text)
    else:
        # Preserve the old preference-only behavior for users whose profile has
        # not been generated yet: role and industry remain binary, while the
        # unavailable profile dimensions are neutral.
        target_roles = _preference_values(user.get("target_roles"))
        target_industries = _preference_values(user.get("target_industries"))
        role_fit = (
            1.0
            if not target_roles
            else float(_contains_preference(role_text, target_roles))
        )
        industry_fit = (
            1.0
            if not target_industries
            else float(_contains_preference(industry_text, target_industries))
        )
        capability_fit = 1.0
        seniority_fit = 1.0

    score_parts = {
        "role": round(MATCH_SCORE_WEIGHTS["role"] * role_fit),
        "industry": round(MATCH_SCORE_WEIGHTS["industry"] * industry_fit),
        "capabilities": round(
            MATCH_SCORE_WEIGHTS["capabilities"] * capability_fit
        ),
        "seniority": round(MATCH_SCORE_WEIGHTS["seniority"] * seniority_fit),
    }
    score = sum(score_parts.values())
    return {
        "score": score,
        "qualifies": True,
        "score_parts": score_parts,
        "role_match": role_fit > 0,
        "industry_match": industry_fit > 0,
        "workplace_match": True,
        "location_match": True,
    }


def generate_profile_for_user(
    conn: psycopg.Connection,
    *,
    user_email: str | None = None,
    user_id: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Generate and persist one profile only after Gemini returns valid data."""
    if (user_email is None) == (user_id is None):
        raise ValueError("pass either user_email or user_id")
    lookup = "email = %s" if user_email is not None else "user_id = %s"
    lookup_value = user_email if user_email is not None else user_id
    row = conn.execute(
        f"""
        SELECT user_id, cv_text, profile_text
        FROM users
        WHERE {lookup}
        """,
        (lookup_value,),
    ).fetchone()
    if row is None:
        raise ValueError("No matching user exists")
    user_id, cv_text, existing_profile_text = row

    generated = gemini_service.generate_user_profile(
        cv_text,
        existing_profile_text=existing_profile_text,
    )
    generated_at = now or datetime.now(timezone.utc)
    with conn.transaction():
        conn.execute(
            """
            UPDATE users
            SET profile_text = %s,
                profile_json = %s,
                profile_generated_at = %s,
                profile_model = %s,
                profile_version = %s,
                profile_source_hash = %s,
                updated_at = %s
            WHERE user_id = %s
            """,
            (
                generated["profile_text"],
                Jsonb(generated["profile_json"]),
                generated_at,
                generated["profile_model"],
                generated["profile_version"],
                generated["profile_source_hash"],
                generated_at,
                user_id,
            ),
        )
    return {**generated, "profile_generated_at": generated_at}


def run_matching(
    conn: psycopg.Connection,
    *,
    user_id: int | None = None,
    user_email: str | None = None,
    now: datetime | None = None,
    published_after: datetime | None = None,
    ats: tuple[str, ...] | None = None,
    board_ids: tuple[int, ...] | None = None,
    model_name: str = MATCH_MODEL_NAME,
    model_version: str = MATCH_MODEL_VERSION,
) -> dict[str, int]:
    """Evaluate open jobs against active profiles and upsert eligible matches.

    All jobs that pass the hard workplace and location filters are upserted as
    ``matched`` with their deterministic score. A previously matched job that
    now fails the hard location filter is marked rejected with a NULL score.
    Closed or excluded jobs are not evaluated or inserted; any existing match
    for one remains unchanged.
    When ``published_after`` is supplied, only jobs in that inclusive window are
    loaded for this run. This keeps recent daily scans bounded without changing
    historical match rows.
    When ``ats`` is supplied, only jobs from those ATSes are evaluated.
    """
    if user_id is not None and user_email is not None:
        raise ValueError("pass either user_id or user_email, not both")
    run_at = now or datetime.now(timezone.utc)
    user_filter = ""
    user_params: tuple[Any, ...] = ()
    if user_id is not None:
        user_filter = " AND user_id = %s"
        user_params = (user_id,)
    elif user_email is not None:
        user_filter = " AND email = %s"
        user_params = (user_email,)

    with conn.transaction():
        users = conn.execute(
            f"""
            SELECT user_id, target_roles, target_industries, base_city,
                   base_country, remote_allowed, onsite_allowed,
                   hybrid_allowed, willing_to_relocate, relocation_cities,
                   relocation_countries, min_match_score, profile_json
            FROM users
            WHERE active{user_filter}
            ORDER BY user_id
            """,
            user_params,
        ).fetchall()
        job_cutoff = ""
        job_params: tuple[Any, ...] = ()
        if published_after is not None:
            job_cutoff = " AND j.published_at >= %s"
            job_params = (published_after,)
        ats_clause = ""
        if ats:
            ats_clause = " AND b.ats = ANY(%s)"
            job_params = (*job_params, list(ats))
        board_clause = ""
        if board_ids:
            board_clause = " AND b.board_id = ANY(%s)"
            job_params = (*job_params, list(board_ids))
        jobs = conn.execute(
            f"""
            SELECT j.job_id, j.title, j.department, j.team, j.company,
                   j.description_text, j.location_raw, j.address, j.is_remote,
                   j.workplace_type, c.industry, c.category
            FROM jobs AS j
            JOIN job_boards AS b ON b.board_id = j.board_id
            LEFT JOIN companies AS c ON c.company_id = b.company_id
            WHERE j.closed_at IS NULL
              AND b.active IS TRUE
            {job_cutoff}
            {ats_clause}
            {board_clause}
            ORDER BY j.job_id
            """,
            job_params,
        ).fetchall()
        feedback_rows = conn.execute(
            """
            SELECT s.user_id, j.job_id, j.title, s.status, f.weight
            FROM user_job_state AS s
            JOIN jobs AS j ON j.job_id = s.job_id
            JOIN feedback_signals AS f
              ON f.user_id = s.user_id AND f.signal = s.status
            WHERE s.status IN ('saved', 'applied', 'rejected')
            """
        ).fetchall()
        feedback_by_user: dict[int, list[tuple[int, str, str, int]]] = {}
        for feedback_user_id, feedback_job_id, title, status, weight in feedback_rows:
            feedback_by_user.setdefault(feedback_user_id, []).append(
                (feedback_job_id, title, status, weight)
            )

        user_columns = (
            "user_id", "target_roles", "target_industries", "base_city",
            "base_country", "remote_allowed", "onsite_allowed",
            "hybrid_allowed", "willing_to_relocate", "relocation_cities",
            "relocation_countries", "min_match_score", "profile_json",
        )
        job_columns = (
            "job_id", "title", "department", "team", "company",
            "description_text", "location_raw", "address", "is_remote",
            "workplace_type", "industry", "category",
        )
        stats = {"evaluated": 0, "matched": 0, "rejected": 0, "skipped": 0}
        for user_row in users:
            user = dict(zip(user_columns, user_row))
            for job_row in jobs:
                job = dict(zip(job_columns, job_row))
                if not _workplace_matches(job, user):
                    conn.execute(
                        """UPDATE job_matches
                           SET score=NULL, match_status='rejected',
                               model_name=%s, model_version=%s, updated_at=%s
                           WHERE user_id=%s AND job_id=%s""",
                        (model_name, model_version, run_at,
                         user["user_id"], job["job_id"]),
                    )
                    stats["skipped"] += 1
                    continue
                if _location_is_configured(user) and not _location_matches(job, user):
                    conn.execute(
                        """
                        UPDATE job_matches
                        SET score = NULL, match_status = 'rejected',
                            model_name = %s, model_version = %s, updated_at = %s
                        WHERE user_id = %s AND job_id = %s
                        """,
                        (
                            model_name,
                            model_version,
                            run_at,
                            user["user_id"],
                            job["job_id"],
                        ),
                    )
                    stats["skipped"] += 1
                    continue
                result = evaluate_job_match(user, job)
                if result is None:
                    stats["skipped"] += 1
                    continue
                result["score"] = max(
                    0,
                    min(
                        100,
                        result["score"]
                        + _feedback_adjustment(
                            job, feedback_by_user.get(user["user_id"], [])
                        ),
                    ),
                )
                stats["evaluated"] += 1
                conn.execute(
                    """
                    INSERT INTO job_matches (
                        user_id, job_id, score, match_status,
                        model_name, model_version, updated_at
                    ) VALUES (%s, %s, %s, 'matched', %s, %s, %s)
                    ON CONFLICT (user_id, job_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        match_status = EXCLUDED.match_status,
                        model_name = EXCLUDED.model_name,
                        model_version = EXCLUDED.model_version,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        user["user_id"],
                        job["job_id"],
                        result["score"],
                        model_name,
                        model_version,
                        run_at,
                    ),
                )
                stats["matched"] += 1
    return stats


def _recommendation_job_hash(job: dict[str, Any]) -> str:
    relevant = {
        field: job.get(field)
        for field in (
            "job_id",
            "title",
            "department",
            "team",
            "company",
            "employment_type",
            "location_raw",
            "address",
            "workplace_type",
            "description_text",
            "job_url",
        )
    }
    serialized = json.dumps(relevant, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _recommendation_job_payload(
    job: dict[str, Any],
    *,
    batch_fit_score: int | None = None,
) -> dict[str, Any]:
    description = str(job.get("description_text") or "").strip()
    if len(description) > 2200:
        description = (
            description[:1500]
            + "\n...[description truncated for batch review]...\n"
            + description[-600:]
        )
    payload = {
        "job_id": job["job_id"],
        "company": job.get("company"),
        "title": job.get("title"),
        "department": job.get("department"),
        "team": job.get("team"),
        "employment_type": job.get("employment_type"),
        "location": job.get("location_raw"),
        "workplace_type": job.get("workplace_type"),
        "description": description[:5000],
        "deterministic_score": job.get("score"),
    }
    if batch_fit_score is not None:
        payload["batch_fit_score"] = batch_fit_score
    return payload


def _recommendation_role_key(job: dict[str, Any]) -> str:
    """Group country-specific ATS variants of the same company role."""
    company = _normalized_search_text(job.get("company"))
    title = _normalized_search_text(job.get("title"))
    title = re.sub(r"\([^)]*\)", " ", title)
    title = re.sub(r"\b(full remote|remote|europe|spain|portugal|app)\b", " ", title)
    title = " ".join(title.split())
    return f"{company}|{title}"


def _review_batch_with_split(
    profile: dict[str, Any],
    jobs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Retry malformed multi-job responses as smaller independently validated batches."""
    try:
        return gemini_service.review_job_batch(
            profile,
            [_recommendation_job_payload(job) for job in jobs],
        )
    except gemini_service.GeminiResponseError:
        if len(jobs) == 1:
            raise
        midpoint = len(jobs) // 2
        return _review_batch_with_split(profile, jobs[:midpoint]) + _review_batch_with_split(
            profile, jobs[midpoint:]
        )


def run_recommendations(
    conn: psycopg.Connection,
    *,
    user_id: int | None = None,
    user_email: str | None = None,
    now: datetime | None = None,
    published_after: datetime | None = None,
    ats: tuple[str, ...] | None = None,
    board_ids: tuple[int, ...] | None = None,
    top_n: int = RECOMMENDATION_TOP_N,
    batch_size: int = RECOMMENDATION_BATCH_SIZE,
    shortlist_size: int = RECOMMENDATION_SHORTLIST_SIZE,
    gemini_review_floor: int = RECOMMENDATION_GEMINI_REVIEW_FLOOR,
    max_gemini_candidates: int = RECOMMENDATION_MAX_GEMINI_CANDIDATES,
    pinned_final_floor: int = RECOMMENDATION_PINNED_FINAL_FLOOR,
    force: bool = False,
) -> dict[str, int]:
    """Review score-floor candidates with Gemini and persist a top recommendation set."""
    if user_id is not None and user_email is not None:
        raise ValueError("pass either user_id or user_email, not both")
    if top_n < 1 or batch_size < 1 or shortlist_size < top_n:
        raise ValueError("invalid recommendation sizing")
    if max_gemini_candidates < 1:
        raise ValueError("max_gemini_candidates must be positive")
    gemini_review_floor = max(0, min(100, int(gemini_review_floor)))
    pinned_final_floor = max(0, min(100, int(pinned_final_floor)))

    user_filter = ""
    user_params: tuple[Any, ...] = ()
    if user_id is not None:
        user_filter = " AND user_id = %s"
        user_params = (user_id,)
    elif user_email is not None:
        user_filter = " AND email = %s"
        user_params = (user_email,)

    user_row = conn.execute(
        f"""
        SELECT user_id, profile_json, profile_source_hash, min_match_score
        FROM users
        WHERE active{user_filter}
        ORDER BY user_id
        LIMIT 1
        """,
        user_params,
    ).fetchone()
    if user_row is None:
        raise ValueError("No active user found for recommendation run")
    user_id_value, profile, profile_source_hash, configured_floor = user_row
    if not isinstance(profile, dict) or not profile:
        raise gemini_service.GeminiConfigurationError(
            "Cannot recommend jobs without a generated profile_json"
        )
    if not profile_source_hash:
        raise gemini_service.GeminiConfigurationError(
            "Cannot recommend jobs without a profile source hash"
        )
    configured_candidate_floor = (
        DEFAULT_RECOMMENDATION_FLOOR
        if configured_floor is None
        else max(0, min(100, int(configured_floor)))
    )
    # This is deliberately independent of the display threshold: deterministic
    # matching remains the source of truth, while Gemini sees only strong fits.
    candidate_floor = max(configured_candidate_floor, gemini_review_floor)
    job_cutoff = ""
    job_params: tuple[Any, ...] = ()
    if published_after is not None:
        job_cutoff = " AND j.published_at >= %s"
        job_params = (published_after,)
    ats_clause = ""
    if ats:
        ats_clause = " AND b.ats = ANY(%s)"
        job_params = (*job_params, list(ats))
    board_clause = ""
    if board_ids:
        board_clause = " AND b.board_id = ANY(%s)"
        job_params = (*job_params, list(board_ids))
    rows = conn.execute(
        f"""
        SELECT j.job_id, j.title, j.department, j.team, j.company,
               j.employment_type, j.location_raw, j.address, j.is_remote,
               j.workplace_type, j.published_at, j.job_url,
               j.description_text, jm.score
        FROM job_matches AS jm
        JOIN jobs AS j ON j.job_id = jm.job_id
        JOIN job_boards AS b ON b.board_id = j.board_id
        LEFT JOIN user_job_state AS s
          ON s.user_id = jm.user_id AND s.job_id = jm.job_id
        WHERE jm.user_id = %s
          AND jm.match_status = 'matched'
          AND COALESCE(s.status, 'new') <> 'rejected'
          AND j.closed_at IS NULL
          AND b.active IS TRUE
          AND jm.score >= %s
          {job_cutoff}
          {ats_clause}
          {board_clause}
        ORDER BY jm.score DESC, j.published_at DESC NULLS LAST, j.job_id
        """,
        (user_id_value, candidate_floor, *job_params),
    ).fetchall()
    columns = (
        "job_id", "title", "department", "team", "company",
        "employment_type", "location_raw", "address", "is_remote",
        "workplace_type", "published_at", "job_url", "description_text", "score",
    )
    candidates = [dict(zip(columns, row)) for row in rows]
    # Preserve the complete deterministic match set in job_matches, but only
    # send a diverse, strongest slice to Gemini.  Role variants and company
    # concentration are reduced before any model call.
    deterministic_candidate_count = len(candidates)
    ordered = sorted(
        candidates,
        key=lambda job: (
            -job["score"],
            -(job["published_at"].timestamp() if job["published_at"] else 0),
            job["job_id"],
        ),
    )
    selected: list[dict[str, Any]] = []
    selected_roles: set[str] = set()
    company_counts: dict[str, int] = {}
    for job in ordered:
        role_key = _recommendation_role_key(job)
        company_key = _normalized_search_text(job.get("company"))
        if role_key in selected_roles or company_counts.get(company_key, 0) >= 2:
            continue
        selected_roles.add(role_key)
        company_counts[company_key] = company_counts.get(company_key, 0) + 1
        selected.append(job)
        if len(selected) >= max_gemini_candidates:
            break
    candidates = selected
    if not candidates:
        empty_hash = hashlib.sha256(json.dumps({
            "profile_source_hash": profile_source_hash,
            "candidate_floor": candidate_floor,
            "gemini_review_floor": gemini_review_floor,
            "max_gemini_candidates": max_gemini_candidates,
            "pinned_final_floor": pinned_final_floor,
            "top_n": top_n,
            "shortlist": [],
        }, sort_keys=True).encode("utf-8")).hexdigest()
        existing_empty = None if force else conn.execute(
            """SELECT run_id FROM profile_recommendation_runs
               WHERE user_id=%s AND profile_source_hash=%s
                 AND published_after IS NOT DISTINCT FROM %s
                 AND candidate_floor=%s AND top_n=%s AND shortlist_hash=%s
               ORDER BY created_at DESC LIMIT 1""",
            (user_id_value, profile_source_hash, published_after,
             candidate_floor, top_n, empty_hash),
        ).fetchone()
        if existing_empty:
            conn.execute(
                """UPDATE job_profile_reviews
                   SET recommendation_run_id=NULL, final_rank=NULL
                   WHERE user_id=%s""", (user_id_value,)
            )
            conn.commit()
            return {"candidate_floor": candidate_floor, "candidates": 0,
                    "batch_reviewed": 0, "batch_cached": 0, "final_candidates": 0,
                    "recommendations": 0, "run_id": existing_empty[0]}
        run_at = now or datetime.now(timezone.utc)
        with conn.transaction():
            conn.execute(
                """UPDATE job_profile_reviews
                   SET recommendation_run_id=NULL, final_rank=NULL
                   WHERE user_id=%s""", (user_id_value,))
            empty_run_id = conn.execute(
                """INSERT INTO profile_recommendation_runs
                   (user_id,profile_source_hash,candidate_floor,top_n,
                    published_after,candidate_count,shortlist_hash,model_name,model_version)
                   VALUES (%s,%s,%s,%s,%s,0,%s,%s,%s) RETURNING run_id""",
                (user_id_value, profile_source_hash, candidate_floor, top_n,
                 published_after, empty_hash, os.environ.get("GEMINI_MODEL", gemini_service.DEFAULT_MODEL),
                 gemini_service.JOB_REVIEW_VERSION),
            ).fetchone()[0]
        return {
            "candidate_floor": candidate_floor,
            "candidates": 0,
            "batch_reviewed": 0,
            "batch_cached": 0,
            "final_candidates": 0,
            "recommendations": 0,
            "run_id": empty_run_id,
        }

    model_name = os.environ.get("GEMINI_MODEL", gemini_service.DEFAULT_MODEL)
    prompt_version = gemini_service.JOB_REVIEW_VERSION
    existing_rows = {
        row[0]: row
        for row in conn.execute(
            """
            SELECT job_id, profile_source_hash, job_source_hash,
                   prompt_version, gemini_model, batch_fit_score,
                   batch_recommendation, batch_strengths, batch_concerns,
                   batch_rationale
            FROM job_profile_reviews
            WHERE user_id = %s
            """,
            (user_id_value,),
        ).fetchall()
    }
    pending: list[dict[str, Any]] = []
    cached = 0
    for job in candidates:
        job["job_source_hash"] = _recommendation_job_hash(job)
        old = existing_rows.get(job["job_id"])
        if (
            not force
            and old is not None
            and old[1] == profile_source_hash
            and old[2] == job["job_source_hash"]
            and old[3] == prompt_version
            and old[4] == model_name
            and old[5] is not None
        ):
            job["batch_review"] = {
                "fit_score": old[5],
                "recommendation": old[6],
                "strengths": old[7] or [],
                "concerns": old[8] or [],
                "rationale": old[9] or "",
            }
            cached += 1
        else:
            pending.append(job)

    # Do not hold the read transaction open while waiting for Gemini. Each
    # successful batch is committed independently below.
    conn.commit()
    run_at = now or datetime.now(timezone.utc)
    for start in range(0, len(pending), batch_size):
        batch = pending[start : start + batch_size]
        reviews = _review_batch_with_split(profile, batch)
        reviews_by_id = {review["job_id"]: review for review in reviews}
        with conn.transaction():
            for job in batch:
                review = reviews_by_id[job["job_id"]]
                conn.execute(
                    """
                    INSERT INTO job_profile_reviews (
                        user_id, job_id, profile_source_hash, job_source_hash,
                        prompt_version, gemini_model, batch_fit_score,
                        batch_recommendation, batch_strengths, batch_concerns,
                        batch_rationale, batch_reviewed_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, job_id) DO UPDATE SET
                        profile_source_hash = EXCLUDED.profile_source_hash,
                        job_source_hash = EXCLUDED.job_source_hash,
                        prompt_version = EXCLUDED.prompt_version,
                        gemini_model = EXCLUDED.gemini_model,
                        batch_fit_score = EXCLUDED.batch_fit_score,
                        batch_recommendation = EXCLUDED.batch_recommendation,
                        batch_strengths = EXCLUDED.batch_strengths,
                        batch_concerns = EXCLUDED.batch_concerns,
                        batch_rationale = EXCLUDED.batch_rationale,
                        batch_reviewed_at = EXCLUDED.batch_reviewed_at,
                        recommendation_run_id = NULL,
                        final_fit_score = NULL,
                        final_rank = NULL,
                        final_recommendation = NULL,
                        final_strengths = NULL,
                        final_concerns = NULL,
                        final_rationale = NULL,
                        final_reviewed_at = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        user_id_value,
                        job["job_id"],
                        profile_source_hash,
                        job["job_source_hash"],
                        prompt_version,
                        model_name,
                        review["fit_score"],
                        review["recommendation"],
                        Jsonb(review["strengths"]),
                        Jsonb(review["concerns"]),
                        review["rationale"],
                        run_at,
                        run_at,
                    ),
                )
                job["batch_review"] = review

    for job in candidates:
        if "batch_review" not in job:
            raise RuntimeError(f"Missing cached review for job {job['job_id']}")
    ordered_candidates = sorted(
        candidates,
        key=lambda job: (
            -job["batch_review"]["fit_score"],
            -job["score"],
            -(job["published_at"].timestamp() if job["published_at"] else 0),
            job["job_id"],
        ),
    )
    shortlist: list[dict[str, Any]] = []
    selected_role_keys: set[str] = set()
    selected_company_counts: dict[str, int] = {}
    for job in ordered_candidates:
        role_key = _recommendation_role_key(job)
        company_key = _normalized_search_text(job.get("company"))
        if (
            role_key in selected_role_keys
            or selected_company_counts.get(company_key, 0) >= 2
        ):
            continue
        selected_role_keys.add(role_key)
        selected_company_counts[company_key] = (
            selected_company_counts.get(company_key, 0) + 1
        )
        shortlist.append(job)
        if len(shortlist) == shortlist_size:
            break
    if len(shortlist) < shortlist_size:
        for job in ordered_candidates:
            role_key = _recommendation_role_key(job)
            if role_key in selected_role_keys:
                continue
            selected_role_keys.add(role_key)
            shortlist.append(job)
            if len(shortlist) == shortlist_size:
                break
    shortlist_signature = [
        (
            job["job_id"],
            job["job_source_hash"],
            job["score"],
            job["batch_review"]["fit_score"],
        )
        for job in shortlist
    ]
    shortlist_hash = hashlib.sha256(
        json.dumps(
            {
                "profile_source_hash": profile_source_hash,
                "candidate_floor": candidate_floor,
                "gemini_review_floor": gemini_review_floor,
                "max_gemini_candidates": max_gemini_candidates,
                "pinned_final_floor": pinned_final_floor,
                "top_n": top_n,
                "ats": list(ats) if ats else None,
                "shortlist": shortlist_signature,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    existing_run = None
    if not force:
        existing_run = conn.execute(
            """
            SELECT run_id
            FROM profile_recommendation_runs
            WHERE user_id = %s
              AND profile_source_hash = %s
              AND published_after IS NOT DISTINCT FROM %s
              AND candidate_floor = %s
              AND top_n = %s
              AND shortlist_hash = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                user_id_value,
                profile_source_hash,
                published_after,
                candidate_floor,
                top_n,
                shortlist_hash,
            ),
        ).fetchone()
    if existing_run:
        pinned_count = conn.execute(
            """
            SELECT count(*)
            FROM job_profile_reviews
            WHERE user_id = %s AND recommendation_run_id = %s
            """,
            (user_id_value, existing_run[0]),
        ).fetchone()[0]
        return {
            "candidate_floor": candidate_floor,
            "candidates": deterministic_candidate_count,
            "batch_reviewed": len(pending),
            "batch_cached": cached,
            "final_candidates": len(shortlist),
            "recommendations": int(pinned_count),
            "run_id": existing_run[0],
        }

    final_reviews = gemini_service.compare_job_shortlist(
        profile,
        [
            _recommendation_job_payload(
                job,
                batch_fit_score=job["batch_review"]["fit_score"],
            )
            for job in shortlist
        ],
    )
    final_by_id = {review["job_id"]: review for review in final_reviews}
    with conn.transaction():
        run_id = conn.execute(
            """
            INSERT INTO profile_recommendation_runs (
                user_id, profile_source_hash, candidate_floor, top_n,
                published_after, candidate_count, shortlist_hash,
                model_name, model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING run_id
            """,
            (
                user_id_value,
                profile_source_hash,
                candidate_floor,
                top_n,
                published_after,
                deterministic_candidate_count,
                shortlist_hash,
                model_name,
                prompt_version,
            ),
        ).fetchone()[0]
        # A fresh run supersedes prior pins. Weak final reviews are retained as
        # evidence but intentionally cannot appear in pinned recommendations.
        conn.execute(
            """
            UPDATE job_profile_reviews
            SET recommendation_run_id = NULL, final_rank = NULL
            WHERE user_id = %s
            """,
            (user_id_value,),
        )
        pinned_jobs = [
            job for job in shortlist
            if int(final_by_id[job["job_id"]]["fit_score"]) >= pinned_final_floor
        ]
        pinned_jobs.sort(
            key=lambda job: (
                int(final_by_id[job["job_id"]].get("rank") or 10**9),
                job["job_id"],
            )
        )
        pinned_ids = {job["job_id"] for job in pinned_jobs}
        pinned_ranks = {
            job["job_id"]: rank
            for rank, job in enumerate(pinned_jobs, 1)
        }
        for job in shortlist:
            review = final_by_id[job["job_id"]]
            is_pinned = job["job_id"] in pinned_ids
            conn.execute(
                """
                UPDATE job_profile_reviews
                SET recommendation_run_id = %s,
                    final_fit_score = %s,
                    final_rank = %s,
                    final_recommendation = %s,
                    final_strengths = %s,
                    final_concerns = %s,
                    final_rationale = %s,
                    final_reviewed_at = %s,
                    updated_at = %s
                WHERE user_id = %s AND job_id = %s
                """,
                (
                    run_id if is_pinned else None,
                    review["fit_score"],
                    pinned_ranks.get(job["job_id"]),
                    review["recommendation"],
                    Jsonb(review["strengths"]),
                    Jsonb(review["concerns"]),
                    review["rationale"],
                    run_at,
                    run_at,
                    user_id_value,
                    job["job_id"],
                ),
            )
    return {
        "candidate_floor": candidate_floor,
        "candidates": deterministic_candidate_count,
        "batch_reviewed": len(pending),
        "batch_cached": cached,
        "final_candidates": len(shortlist),
        "recommendations": min(top_n, len(pinned_ids)),
        "run_id": run_id,
    }


def _run(args: argparse.Namespace) -> int:
    match_requested = getattr(args, "match", False)
    recommend_requested = getattr(args, "recommend", False)
    profile_requested = getattr(args, "generate_profile", False)
    export_requested = getattr(args, "export_report", False)
    recommendation_export_requested = (
        getattr(args, "export_recommendations", False) or recommend_requested
    )
    match_requested = match_requested or recommend_requested
    matching_only = (
        match_requested
        and not args.board
        and not args.boards_from
        and not getattr(args, "daily", False)
    )
    profile_only = profile_requested and not args.board and not args.boards_from
    specs = [] if matching_only or profile_only else _board_specs(args)
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
    total_pruned = 0
    failed = 0
    unchanged = 0
    daily_import_run_id: int | None = None
    daily_failed_boards = 0
    daily_board_count = 0
    worker_id = _daily_worker_id()

    with psycopg.connect(dsn) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()
        if args.daily:
            selected_ats = _ats_list(args.ats)
            with conn.cursor() as cur:
                added_boards = _sync_board_registry(
                    cur,
                    specs,
                    datetime.now(timezone.utc),
                )
            if added_boards:
                conn.commit()
                print(f"Daily board registry: added {added_boards} newly discovered boards")
            database_specs = _database_board_specs(conn, selected_ats)
            if database_specs or _database_has_board_rows(conn, selected_ats):
                specs = database_specs
                print(
                    f"Daily board registry: {len(specs)} active PostgreSQL boards "
                    f"({', '.join(f'{ats}={sum(item[0] == ats for item in specs)}' for ats in selected_ats)})"
                )
            # The registry SELECTs above start an implicit transaction. End it
            # before importing so each board write below can commit independently.
            conn.commit()
            (
                daily_import_run_id,
                daily_scan_items,
                resumed,
                daily_board_count,
                effective_published_after,
            ) = _prepare_daily_import_run(
                conn,
                selected_ats,
                published_after,
                auto_resume=getattr(args, "auto_resume", False),
                worker_id=worker_id,
            )
            published_after = effective_published_after
            completed_before = daily_board_count - len(daily_scan_items)
            action = "resuming" if resumed else "started"
            print(
                f"Daily import run {daily_import_run_id}: {action}; "
                f"{completed_before}/{daily_board_count} boards already accounted for, "
                f"{len(daily_scan_items)} pending/retry"
            )
        else:
            daily_scan_items = [
                (position, 0, ats, slug)
                for position, (ats, slug) in enumerate(specs, 1)
            ]
        if profile_requested:
            try:
                generated = generate_profile_for_user(
                    conn,
                    user_email=args.user_email,
                )
            except (ValueError, gemini_service.GeminiProfileError) as exc:
                print(f"Profile generation failed: {exc}", file=sys.stderr)
                return 1
            print(
                "Profile generation complete: "
                f"{generated['profile_model']} {generated['profile_version']} "
                f"for {args.user_email}"
            )
            return 0

        if daily_import_run_id is not None:
            parallel_stats = _run_daily_boards_parallel(
                dsn,
                daily_scan_items,
                daily_board_count,
                daily_import_run_id,
                published_after,
                greenhouse_content,
                worker_id,
                getattr(args, "auto_resume", False),
            )
            total_jobs += parallel_stats["jobs"]
            total_new += parallel_stats["new"]
            total_updated += parallel_stats["updated"]
            total_closed += parallel_stats["closed"]
            unchanged += parallel_stats["unchanged"]
            failed += parallel_stats["failed"]
            total_pruned += parallel_stats["pruned"]
            # The parallel worker has consumed the entire immutable snapshot.
            # Leave the existing loop for explicitly scoped, non-daily imports.
            daily_scan_items = []

        for position, tracked_board_id, ats, slug in daily_scan_items:
            try:
                print(
                    f"{position}/{daily_board_count or len(specs)} {ats}/{slug}: fetching...",
                    flush=True,
                )
                board_id, stored_etag, cached_published_after = _board_fetch_state(
                    conn, ats, slug
                )
                conditional_etag = (
                    stored_etag
                    if stored_etag
                    and _etag_covers(cached_published_after, published_after)
                    else None
                )
                fetch_meta: dict[str, Any] = {}
                cached_workday_jobs = (
                    _cached_workday_jobs(conn, board_id, slug)
                    if ats == "workday"
                    else None
                )
                # Psycopg starts an implicit transaction for the state reads above.
                # Without ending it here, conn.transaction() creates only a
                # savepoint and every board remains uncommitted until the full run
                # reaches matching/recommendations.
                conn.commit()
                rows, skipped = _fetch_normalized(
                    ats,
                    slug,
                    published_after,
                    greenhouse_content=greenhouse_content,
                    etag=conditional_etag,
                    meta=fetch_meta,
                    cached_workday_jobs=cached_workday_jobs,
                )
                with conn.transaction():
                    with conn.cursor() as cur:
                        seen_at = datetime.now(timezone.utc)
                        board_id = _ensure_board(cur, ats, slug, seen_at)
                        company = _board_company_name(cur, board_id)
                        existing = _count_existing(cur, rows)
                        new = len(rows) - existing
                        _, closed = _upsert_board_jobs(
                            cur,
                            board_id,
                            company,
                            rows,
                            seen_at,
                            close_missing=published_after is None,
                        )
                        _save_board_etag(
                            cur,
                            board_id,
                            fetch_meta.get("etag"),
                            seen_at,
                            published_after,
                        )
                        if daily_import_run_id is not None:
                            _record_daily_board_success(
                                cur,
                                daily_import_run_id,
                                board_id,
                                fetched=len(rows),
                                inserted=new,
                                updated=existing,
                                worker_id=worker_id,
                            )
                total_jobs += len(rows)
                total_new += new
                total_updated += existing
                total_closed += closed
                print(
                    f"{position}/{daily_board_count or len(specs)} {ats}/{slug}: "
                    f"{len(rows)} jobs ({new} new, {existing} updated, "
                    f"{skipped} before cutoff, {closed} closed)"
                )
            except job_boards.NotModified:
                unchanged += 1
                with conn.transaction():
                    with conn.cursor() as cur:
                        seen_at = datetime.now(timezone.utc)
                        if board_id is not None:
                            _mark_board_unchanged(
                                cur,
                                board_id,
                                seen_at,
                                published_after,
                            )
                            if daily_import_run_id is not None:
                                _record_daily_board_success(
                                    cur,
                                    daily_import_run_id,
                                    board_id,
                                    fetched=0,
                                    inserted=0,
                                    updated=0,
                                    status="completed",
                                    worker_id=worker_id,
                                )
                print(
                    f"{position}/{daily_board_count or len(specs)} "
                    f"{ats}/{slug}: unchanged (304, ETag)"
                )
            except job_boards.NotFound as exc:
                failed += 1
                if daily_import_run_id is not None:
                    _record_daily_board_failure(
                        conn,
                        daily_import_run_id,
                        tracked_board_id,
                        exc,
                        worker_id=worker_id,
                        automatic_retry=getattr(args, "auto_resume", False),
                    )
                print(
                    f"{position}/{daily_board_count or len(specs)} "
                    f"{ats}/{slug}: 404",
                    file=sys.stderr,
                )
            except Exception as exc:
                failed += 1
                if daily_import_run_id is not None:
                    _record_daily_board_failure(
                        conn,
                        daily_import_run_id,
                        tracked_board_id,
                        exc,
                        worker_id=worker_id,
                        automatic_retry=getattr(args, "auto_resume", False),
                    )
                print(
                    f"{position}/{daily_board_count or len(specs)} {ats}/{slug}: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

        if daily_import_run_id is not None:
            daily_counts = _daily_import_counts(conn, daily_import_run_id)
            daily_failed_boards = daily_counts["failed"]
            print(
                "Daily import board accounting: "
                f"{daily_board_count} total = {daily_counts['completed']} completed + "
                f"{daily_counts['empty']} empty + {daily_failed_boards} failed + "
                f"{daily_counts['pending']} pending"
            )
            try:
                from scripts.export_daily_import_failures import (
                    export_daily_import_failures,
                )

                report_paths = export_daily_import_failures(
                    dsn,
                    daily_import_run_id,
                )
                print(
                    "Daily failed-board report: "
                    f"{report_paths['csv'].relative_to(Path.cwd())} and "
                    f"{report_paths['json'].relative_to(Path.cwd())}"
                )
            except Exception as exc:
                print(
                    f"Daily failed-board report could not be written: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
            failures_allowed = _daily_failures_allow_downstream(
                daily_failed_boards, daily_board_count
            )
            if daily_counts["pending"] or not failures_allowed:
                failure_rate = (
                    daily_failed_boards * 100 / daily_board_count
                    if daily_board_count
                    else 0
                )
                print(
                    f"Daily import run {daily_import_run_id} remains incomplete; "
                    f"failed boards are {failure_rate:.2f}% of the snapshot "
                    f"(the continuation threshold is at least "
                    f"{DAILY_MIN_SUCCESS_PERCENT}% successful). "
                    "Rerun the same command to retry only failed/pending boards. "
                    "Matching, recommendations, and reports were not started.",
                    file=sys.stderr,
                )
                return 1
            if daily_failed_boards:
                print(
                    f"Daily import run {daily_import_run_id}: continuing with "
                    f"{daily_failed_boards}/{daily_board_count} failed boards "
                    f"(at least {DAILY_MIN_SUCCESS_PERCENT}% of boards succeeded)"
                )
        if match_requested:
            match_stats = run_matching(
                conn,
                published_after=published_after,
                ats=tuple(_ats_list(args.ats)),
            )
            print(
                "\nMatching complete: "
                f"{match_stats['evaluated']} evaluated, "
                f"{match_stats['matched']} matched, "
                f"{match_stats['skipped']} excluded"
            )
        if recommend_requested:
            try:
                recommendation_stats = run_recommendations(
                    conn,
                    published_after=published_after,
                    ats=tuple(_ats_list(args.ats)),
                    batch_size=args.recommend_batch_size,
                )
            except (ValueError, gemini_service.GeminiProfileError) as exc:
                print(f"\nRecommendation generation failed: {exc}", file=sys.stderr)
                failed += 1
            else:
                print(
                    "\nRecommendations complete: "
                    f"{recommendation_stats['candidates']} candidates at or above "
                    f"{recommendation_stats['candidate_floor']}, "
                    f"{recommendation_stats['batch_reviewed']} Gemini-reviewed, "
                    f"{recommendation_stats['batch_cached']} cached, "
                    f"{recommendation_stats['recommendations']} top recommendations"
                )

    if export_requested:
        try:
            from scripts.export_match_reports import _export

            selected_ats = _ats_list(args.ats)
            if args.daily and getattr(args, "daily_all_ats", False):
                _export(published_after=published_after)
                for ats in selected_ats:
                    _export(published_after=published_after, ats=ats)
            elif args.ats == "all":
                _export(published_after=published_after)
            else:
                for ats in selected_ats:
                    _export(published_after=published_after, ats=ats)
        except Exception as exc:
            print(f"\nReport export failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            failed += 1
    if recommendation_export_requested:
        try:
            from scripts.export_match_reports import _export_recommendations

            selected_ats = _ats_list(args.ats)
            if args.daily and getattr(args, "daily_all_ats", False):
                _export_recommendations(published_after=published_after)
                for ats in selected_ats:
                    _export_recommendations(published_after=published_after, ats=ats)
            elif args.ats == "all":
                _export_recommendations(published_after=published_after)
            else:
                for ats in selected_ats:
                    _export_recommendations(published_after=published_after, ats=ats)
        except Exception as exc:
            print(
                f"\nRecommendation export failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            failed += 1

    if daily_import_run_id is not None:
        downstream_failed = failed > daily_failed_boards
        if downstream_failed:
            print(
                f"Daily import run {daily_import_run_id} remains incomplete because "
                "matching, recommendation, or report processing failed; rerun the "
                "same command to continue.",
                file=sys.stderr,
            )
        else:
            _finish_daily_import_run(
                dsn,
                daily_import_run_id,
                failed_boards=daily_failed_boards,
                downstream_failed=False,
            )
            final_status = (
                "completed_with_errors" if daily_failed_boards else "completed"
            )
            print(f"Daily import run {daily_import_run_id}: {final_status}")
            # Board failures below the configured threshold are part of the
            # completed_with_errors outcome, not a process failure. The
            # scheduled runner uses this exit code to decide whether it may
            # continue with per-user matching and recommendations.
            return 1 if downstream_failed else 0

    if specs:
        print(
            f"\nPostgreSQL import complete: {total_jobs} current jobs, "
            f"{total_new} new, {total_updated} updated, {total_closed} closed, "
            f"{unchanged} unchanged, {total_pruned} Workable boards deactivated, "
            f"{failed} failed boards"
        )
    elif not match_requested and not recommend_requested:
        print(
            f"\nPostgreSQL import complete: {total_jobs} current jobs, "
            f"{total_new} new, {total_updated} updated, {total_closed} closed, "
            f"{unchanged} unchanged, {total_pruned} Workable boards deactivated, "
            f"{failed} failed boards"
        )
    return 1 if failed else 0


def run(args: argparse.Namespace) -> int:
    """Serialize daily runs, while leaving explicitly scoped imports independent."""
    if not getattr(args, "daily", False):
        return _run(args)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set; use the Replit-managed database")
    # A global lock is intentional: an all-ATS run overlaps every ATS-specific
    # scope, so scope-specific locks would still allow the same boards to run twice.
    with psycopg.connect(dsn, autocommit=True) as lock_conn:
        acquired = lock_conn.execute(
            "SELECT pg_try_advisory_lock(hashtext('daily-import'))"
        ).fetchone()[0]
        if not acquired:
            raise SystemExit(
                "another daily import is already running; wait for it to finish "
                "or stop it before starting a new one"
            )
        return _run(args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persist selected job boards in Replit PostgreSQL."
    )
    parser.add_argument(
        "--ats",
        default=None,
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
        "--workday-bruteforce",
        action="store_true",
        help="when discovering Workday, also probe fallback board names for archived tenants",
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
    parser.add_argument(
        "--match",
        action="store_true",
        help=(
            "run deterministic preference matching after import; when used "
            "alone, do not fetch any boards"
        ),
    )
    parser.add_argument(
        "--recommend",
        action="store_true",
        help=(
            "match eligible jobs, review score-floor candidates with Gemini, and "
            "export Konstantin's cached top recommendations"
        ),
    )
    parser.add_argument(
        "--recommend-batch-size",
        type=int,
        default=RECOMMENDATION_BATCH_SIZE,
        help="number of jobs sent to Gemini per batch during recommendations",
    )
    parser.add_argument(
        "--daily",
        action="store_true",
            help=(
            "scan cached Ashby, Greenhouse, Lever, SmartRecruiters, and Workday boards, "
            "match immediately, "
            "and export ATS-specific reports; combine with --ats to select one "
            "or more ATSes; defaults to the last 7 days"
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "explicitly request daily resume behavior; --daily already resumes the "
            "latest incomplete run with the same ATS scope and cutoff automatically"
        ),
    )
    parser.add_argument(
        "--auto-resume",
        action="store_true",
        help=(
            "in scheduled daily mode, reclaim the oldest expired incomplete run "
            "for the ATS scope before creating a new run"
        ),
    )
    parser.add_argument(
        "--export-report",
        action="store_true",
        help="export the matched-role CSV and JSON report for Konstantin after matching",
    )
    parser.add_argument(
        "--export-recommendations",
        action="store_true",
        help="export the latest Gemini-ranked recommendation report for Konstantin",
    )
    parser.add_argument(
        "--generate-profile",
        action="store_true",
        help=(
            "generate and save an evidence-based AI profile from a user's CV; "
            "use with --user-email and no board/import options"
        ),
    )
    parser.add_argument(
        "--user-email",
        help="user email for --generate-profile",
    )
    args = parser.parse_args()
    if args.daily:
        daily_all_ats = args.ats is None
        if args.board or args.boards_from or args.limit is not None:
            parser.error(
                "--daily cannot be combined with --board, --boards-from, or --limit"
            )
        if args.greenhouse_content:
            parser.error(
                "--daily never requests Greenhouse descriptions; omit --greenhouse-content"
            )
        if args.ats is None:
            args.ats = ",".join(ATS_NAMES)
        _ats_list(args.ats)
        args.daily_all_ats = daily_all_ats
        args.match = True
        args.recommend = True
        args.export_report = True
        args.export_recommendations = True
        if not args.published_after:
            args.published_after = (
                datetime.now(timezone.utc) - timedelta(days=7)
            ).date().isoformat()
    elif args.resume:
        parser.error("--resume requires --daily")
    elif args.auto_resume:
        parser.error("--auto-resume requires --daily")
    elif args.ats is None:
        args.ats = "all"
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.recommend_batch_size < 1:
        parser.error("--recommend-batch-size must be at least 1")
    if args.generate_profile:
        if not args.user_email:
            parser.error("--generate-profile requires --user-email")
        if (
            args.match
            or args.recommend
            or args.board
            or args.boards_from
            or args.limit is not None
            or args.published_after
            or args.greenhouse_content
            or args.daily
            or args.export_report
            or args.export_recommendations
        ):
            parser.error(
                "--generate-profile cannot be combined with import or --match options"
            )
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()