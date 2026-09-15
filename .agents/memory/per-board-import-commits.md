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