---
name: Workable empty-board pruning
description: The safety rule for removing inactive Workable accounts during daily imports.
---

Only prune a Workable board when its successful widget response contains zero jobs in total. A response with jobs that are all older than the report cutoff must remain active.

**Why:** The importer applies the published-after cutoff before normalization, so an empty eligible-job list does not prove that the provider account is inactive. Provider errors and throttling are also inconclusive.

**How to apply:** For confirmed zero-job responses, deactivate the PostgreSQL registry row and remove the account from the runtime boards cache. Keep historical job rows; do not cascade-delete stored history.