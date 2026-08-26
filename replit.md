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
