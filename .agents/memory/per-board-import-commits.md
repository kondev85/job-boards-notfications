---
name: Per-board import commits
description: Transaction boundary required to preserve progress during long PostgreSQL board imports.
---

End implicit PostgreSQL read transactions before entering each board's write transaction.
Otherwise the intended write transaction becomes a nested savepoint inside one run-wide
transaction, so completed boards remain invisible and are rolled back if the import stops.

**Why:** Psycopg begins a transaction even for board-state reads. A long Workday run appeared
to process hundreds of boards, but no jobs were visible and interruption discarded all job
writes since the run began.

**How to apply:** Keep network fetching outside transactions. After reading fetch state or
cached evidence, close the read transaction; then perform each board's upserts in a fresh
top-level transaction. Regression tests should observe committed progress from a second
connection after a later board is interrupted.

Long daily imports use a PostgreSQL run record keyed by the canonical ATS scope and exact
cutoff, with an immutable ordered board snapshot. Completed and empty boards are skipped on
resume. Failures strictly below 1% of the snapshot allow downstream work and a
`completed_with_errors` result; at 1% or more they remain retryable. Only one daily import
may run at a time because an all-ATS scope overlaps every subset.

**Why:** Workspace restarts can interrupt multi-hour Workday scans, and a mutable current
registry or latest job ID cannot reliably identify progress, especially for empty boards.

**How to apply:** Daily commands resume automatically when scope and cutoff match an
incomplete run. A changed scope/cutoff or a completed prior run starts a fresh snapshot.