# job-boards (imported project)

A dependency-free Python CLI that scrapes public job postings from Ashby, Greenhouse,
and Lever job boards. See `README.md` for the full product description and
`openwiki/quickstart.md` for the engineering map.

## Running on Replit

This is a CLI tool, not a web server — there is no long-running process, so no
Replit workflow is configured. Run it on demand from the Shell.

`uv` and Python 3.12 are already available in this environment; the script pins its
own dependencies (none) via PEP 723 inline metadata, so no install step is needed.

```bash
# Identify your traffic (recommended before real network runs)
export JOB_SCRAPER_CONTACT="you@example.com"

# Offline self-check (58 tests, no network)
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

Verified in this environment: `uv run test_job_boards.py` passes (58/58), and a live
test scrape (`--ats greenhouse --title "engineer" --limit 5`) successfully hit the
network and returned 442 matching postings.

No core logic has been modified — the project runs as imported.

## PostgreSQL persistence (v1)

Replit's managed PostgreSQL database is available through `DATABASE_URL`; no
database installation or external credentials are needed. The original SQLite
scraper remains available and is not replaced. `postgres_persistence.py` is a
separate persistence layer that reuses the scraper's ATS adapters and creates
only these PostgreSQL tables:

```text
companies
job_boards
jobs
```

The initial company row is source-scoped to each `(ats, slug)` board. The
persistence layer does not automatically merge companies across ATS platforms.
Jobs are unique by `(ats, external_id)`. Ashby and Lever descriptions are stored
as normalized plain text; Greenhouse descriptions remain empty until a separate
enrichment strategy is approved. `source_updated_at` is populated only when an
upstream payload supplies an updated/modified timestamp.

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
