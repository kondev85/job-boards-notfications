# job-boards (imported project)

A dependency-free Python CLI that scrapes public job postings from Ashby, Greenhouse,
and Lever job boards. See `README.md` for the full product description and
`openwiki/quickstart.md` for the engineering map.

## Running on Replit

This is a CLI tool, not a web server — there is no long-running process, so no
Replit workflow is configured. Run it on demand from the Shell.

`uv` and Python 3.12 are already available in this environment; the scraper and its
original tests pin no dependencies via PEP 723 inline metadata. The PostgreSQL
regression script provisions its `psycopg` dependency automatically when run with
`uv`.

```bash
# Identify your traffic (recommended before real network runs)
export JOB_SCRAPER_CONTACT="you@example.com"


# Offline self-check (58 scraper tests, no network)
uv run test_job_boards.py

# Offline self-check (58 scraper tests, no network)
uv run test_job_boards.py

# Small test scrape (5 Greenhouse boards, title match "engineer")
uv run job_boards.py --ats greenhouse --title "engineer" --limit 5 --out test-scrape

# Full scrape, every platform, every posting (uses cached boards.seed.json)
uv run job_boards.py --all

# Refresh the discovered board list first (slower, monthly cadence per README)
uv run job_boards.py --refresh-boards --all
```

Output goes to `job-boards.csv`, `job-boards.json`, and an accumulating
`job-boards.db` SQLite file (all gitignored by design — see `.gitignore` comments).

Verified in this environment: `uv run test_job_boards.py` passes (58/58), and the
PostgreSQL regression suite covers adapter mappings plus a temporary local database.
A live test scrape (`--ats greenhouse --title "engineer" --limit 5`) successfully hit
the network and returned 442 matching postings.

No core logic has been modified — the project runs as imported.

## PostgreSQL persistence (v1)

Replit's managed PostgreSQL database is available through `DATABASE_URL`; no
database installation or external credentials are needed. The original SQLite
scraper remains available and is not replaced. `postgres_persistence.py` is a
separate persistence layer that reuses the scraper's ATS adapters and creates
these PostgreSQL tables:

```text
companies
job_boards
jobs
users
job_matches
```

The initial company row is source-scoped to each `(ats, slug)` board. The
persistence layer does not automatically merge companies across ATS platforms.
Jobs are unique by `(ats, external_id)`. Ashby and Lever descriptions are stored
as normalized plain text; Greenhouse descriptions are populated when the
scoped `--greenhouse-content` enrichment option is used. `source_updated_at` is
populated only when an upstream payload supplies an updated/modified timestamp.
`jobs.address` is JSONB provider evidence: Ashby stores primary and secondary
locations, Greenhouse stores location/offices/metadata, and Lever stores country
and all listed locations.

`jobs.company` is a denormalized display snapshot of the related
`companies.display_name`, while `jobs.board_id` and
`job_boards.company_id` remain the canonical relationship. Greenhouse department
and team are filled only when a board supplies explicitly named custom metadata;
the fields remain nullable when that metadata is absent.

`users` stores matching profiles and their location, workplace, relocation, region,
and score-threshold preferences. Schema initialization seeds one default active
profile for Konstantin Kondev (`infobettor@gmail.com`) from the supplied CV and
profile specification. The seed uses the email as its stable key, so rerunning an
import updates that profile instead of creating a duplicate. Estepona's latitude
and longitude remain NULL because the supplied `36.xxxxxx` and `-5.xxxxxx` values
are placeholders, not verified coordinates.

`users.profile_json` stores the generic, evidence-based AI profile extracted from
`cv_text`. `profile_text` is the concise readable summary from that profile.
Normal imports preserve a generated or manually edited `profile_text` and `cv_text`;
only an explicit profile-generation action updates them. The extraction layer does
not infer job preferences or alter matching scores.

`job_matches` links a user to a job with a nullable score, match status, optional
feedback/model metadata, notification timestamp, and audit timestamps. A
`(user_id, job_id)` unique constraint prevents duplicate matches; deleting either
the user or job cascades to its matches.

Run the deterministic preference matcher after importing jobs:

```bash
uv run postgres_persistence.py --match
```

The matcher considers active users and jobs whose `closed_at` is NULL. It first
rejects jobs whose location evidence contains an excluded region, whose workplace
type is not allowed, or whose location is incompatible with the user's configured
city/country/region rules. Location is a hard filter and contributes no points,
so a location-incompatible job cannot enter the ranking. For remote jobs, explicit
provider address evidence and explicit location labels are preferred over text.
European evidence (such as an EU country, Europe, or EMEA) can match a European
user; explicit US evidence cannot. A plain Remote/Anywhere/Work from home posting
with no geographic evidence is rejected for configured non-US users, but remains
eligible for an explicitly US-targeting user. Users with no location preference
remain unconstrained. Greenhouse description evidence is used when content has
been enriched. Numeric distance limits are not calculated because the schema does
not contain verified job coordinates.

Remaining jobs receive a deterministic 100-point score: role fit is 40 points,
industry fit 25, profile capabilities/tools/transferable evidence 20, and
seniority/leadership alignment 15. When `profile_json` exists, role and industry
fit combine explicit user preferences with the profile's evidence, while
capability and seniority components use only structured profile data. Users
without `profile_json` retain the legacy preference-only score behavior. Industry
fit uses explicit company metadata first and conservative title/description text
as a fallback.

Only jobs at or above `min_match_score` create or update a `matched` row. A
below-threshold job does not create a new row; if it already has a match, that
row is updated to `rejected` with the latest score and matcher metadata.
Closed or excluded jobs are skipped and do not create or alter match rows.
Matching writes `model_name = 'deterministic-preferences'` and
`model_version = 'v2-profile-aware'` because those values identify this scoring
workflow. Rerunning the command updates the same `(user_id, job_id)` row and does
not create duplicates. The matched-role CSV and JSON reports order roles by score
descending and include a recalculated per-report rank. This workflow does not call
an AI model, send notifications, or schedule notification delivery.

### AI profile extraction

Generate a profile manually for a user whose CV is already stored in
`users.cv_text`:

```bash
uv run postgres_persistence.py \
  --generate-profile \
  --user-email user@example.com
```

This requires the `GEMINI_API_KEY` secret. `GEMINI_MODEL` is optional and defaults
to `gemini-2.5-flash`. The command stores the validated `profile_json`, its concise
`profile_text`, the model/version, generation timestamp, and a hash of the CV source.
It does not run during ordinary job imports or matching. Failed requests and invalid
responses leave the existing profile unchanged. The profile schema is profession- and
industry-agnostic; explicit job preferences remain separate from CV-derived evidence.

### Safe smoke import

This imports exactly three boards and is the checkpoint before any large import:

```bash
uv run postgres_persistence.py \
  --board ashby:abridge \
  --board ashby:linear \
  --board greenhouse:stripe
```

The command creates the schema if needed, imports current listed jobs, and
upserts repeat runs without duplicates. It does not run the full Ashby import.

### Greenhouse descriptions

The normal Greenhouse endpoint omits descriptions. To enrich an explicitly
selected board, request the larger `content=true` response:

```bash
uv run postgres_persistence.py \
  --board greenhouse:stripe \
  --greenhouse-content
```

This updates the existing jobs' `description_text` values with plain text and
does not erase other metadata. Keep the board list scoped while testing because
Greenhouse content responses are substantially larger than the normal list
response. Department and team are populated only from explicitly named
Greenhouse metadata fields and otherwise remain nullable.

To import the bounded Greenhouse seed list while preserving all existing rows
older than July 27, 2026:

```bash
uv run postgres_persistence.py \
  --boards-from boards.seed.json \
  --ats greenhouse \
  --published-after 2026-07-27 \
  --greenhouse-content
```

With `--published-after`, the importer does not close or delete older jobs.
Repeating the command makes one sequential API request per selected board and
upserts jobs by `(ats, external_id)`.

### Daily Ashby + Greenhouse scan

Run the lightweight daily scan, deterministic matching, and Konstantin report
export as one command:

```bash
uv run postgres_persistence.py \
  --daily \
  --published-after 2026-08-26
```

`--daily` selects the cached Ashby and Greenhouse boards, defaults to the last
seven days when no cutoff is supplied, runs the deterministic matcher, and writes
`reports/matched_roles.csv` and `reports/matched_roles.json` for
`infobettor@gmail.com`. It never requests Greenhouse `content=true`, so
Greenhouse descriptions remain deferred. Matching and report export use the
same inclusive cutoff, so the daily report contains only roles in that window.

PostgreSQL stores the latest board ETag and the cutoff covered by that response.
If the board is unchanged, a later compatible scan receives `304 Not Modified`
and skips payload parsing and job writes. A scan with an older cutoff
automatically makes a full request instead of incorrectly trusting a narrower
cached response. The existing `--greenhouse-content` option remains an explicit,
manual full-board enrichment operation and is not part of the daily command.

Inspect the PostgreSQL database from Replit's My Data pane, or use SQL such as:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT jb.ats, jb.slug, COUNT(j.job_id) AS jobs
FROM job_boards jb
LEFT JOIN jobs j ON j.board_id = jb.board_id
GROUP BY jb.ats, jb.slug
ORDER BY jb.ats, jb.slug;

SELECT ats, external_id, title, description_text IS NOT NULL AS has_description
FROM jobs
ORDER BY job_id
LIMIT 10;

SELECT company, department, team, COUNT(*) AS jobs
FROM jobs
GROUP BY company, department, team
ORDER BY company
LIMIT 20;

SELECT user_id, name, email, active, is_default, base_city, base_country,
       min_match_score
FROM users
ORDER BY user_id;

SELECT jm.match_id, u.email, j.ats, j.external_id, jm.score,
       jm.match_status, jm.notified_at
FROM job_matches jm
JOIN users u ON u.user_id = jm.user_id
JOIN jobs j ON j.job_id = jm.job_id
ORDER BY jm.match_id
LIMIT 20;
```

### Full import (run only after the smoke-test review)

```bash
uv run postgres_persistence.py --ats ashby --published-after 2026-07-15
```

This uses the locally cached `boards.json` registry. The full command is
intentionally documented but is not run as part of the smoke-test checkpoint.
The cutoff is inclusive: jobs with `published_at` on July 15, 2026 or later are
eligible; jobs with an older or missing `published_at` are skipped before any
database upsert. A cutoff run does not close existing jobs that were omitted
because of the filter.

# PostgreSQL persistence regressions (offline fixtures + temporary local database)
uv run test_postgres_persistence.py
