---
name: Workday adapter boundaries
description: Durable constraints for Workday discovery and shared job identity.
---

Workday requisition IDs are only tenant- or board-scoped, so the shared external ID must
include the verified board identifier before persistence. Discovery should prefer archived
career-site paths plus live CXS verification; fallback board-name probing stays opt-in.
Workday CDX results must be paginated; a single response is an alphabetically truncated
slice and can make the board registry appear to stop at a particular company prefix.
The CXS list API is capped at 20 results, so cutoff imports should skip unnecessary detail
requests and use only modest bounded concurrency for the detail pages that remain.
Persisted Workday detail evidence should be reused on later imports because Workday does not
provide a dependable board ETag.
Board discovery is separate from job scanning; the daily PostgreSQL path must register new
cache entries without reactivating boards an administrator disabled.
Automatically discovered boards belong in the gitignored cache and PostgreSQL; the committed
seed remains a small manual fallback rather than a generated customer directory.

**Why:** Workday has no public board directory, and the same requisition ID can occur on
different customers. Unbounded fallback probing can also create thousands of requests, while
per-job detail enrichment can make a board import look stalled. Large shared boards can have
thousands of postings and no list-level publication dates.

**How to apply:** Keep Workday-specific discovery, request pacing, and ID construction
inside the adapter boundary; matching, persistence, and recommendation code should remain
ATS-agnostic.