---
name: Board registry maintenance schema
description: The PostgreSQL job_boards table does not include an updated_at column.
---

Board registry status changes must update the `active` field without writing
`updated_at`; the board table uses first/last-seen timestamps instead.

**Why:** The existing maintenance command failed against the live schema before
the status update because it assumed an `updated_at` column that does not exist.

**How to apply:** Keep maintenance SQL for job_boards limited to columns present
in the canonical schema, especially when deactivating failed boards.