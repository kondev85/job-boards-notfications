---
name: Deferred Workday enrichment throughput
description: Deferred detail enrichment can produce a large current-run backlog after list importing completes.
---

Keep deferred Workday enrichment observable with per-board progress, elapsed time, and jobs-per-second logging. A completed list-import phase can still leave tens of thousands of current-run jobs without descriptions, so a stale run heartbeat after board accounting may mean detail enrichment is active rather than blocked.

**Why:** The importer intentionally separates Workday list persistence from detail requests, and the detail phase begins only after all board list work finishes. Without separate progress output, the final board log looks like the importer has stopped.

**How to apply:** When changing deferred enrichment concurrency or batching, preserve shell-visible progress and distinguish list completion from detail completion.