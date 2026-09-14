---
name: Workday adapter boundaries
description: Durable constraints for Workday discovery and shared job identity.
---

Workday requisition IDs are only tenant- or board-scoped, so the shared external ID must
include the verified board identifier before persistence. Discovery should prefer archived
career-site paths plus live CXS verification; fallback board-name probing stays opt-in.
Workday CDX results must be paginated; a single response is an alphabetically truncated
slice and can make the board registry appear to stop at a particular company prefix.
The CDX `showNumPages` request must not include `fl=original`: current Wayback responses
then contain an `original: null` row instead of a page count. Treat malformed counts,
page failures, and safety-limit overflow as an incomplete run, never as page one.
Long Workday CDX crawls must checkpoint completed pages and extracted candidates so a
Wayback rate limit can be resumed without replaying hundreds of successful requests.
Keep that checkpoint until candidate validation succeeds and the board cache is written;
request errors must preserve both the old cache and the completed crawl for a cheap retry.
Persist validation outcomes in small batches and atomically union confirmed-live boards into
the cache immediately. Partial runs may add verified boards but must never remove old ones.
Treat definitive candidate failures such as missing boards and Workday HTTP 422 responses as
reusable invalid outcomes; reserve retryable request errors for uncertain transport, server,
rate-limit, and access-denied failures.
An isolated CDX page failure should be checkpointed and skipped temporarily; abort after
three consecutive failures, and never mark the crawl complete until every page is resolved.
When Wayback is broadly unavailable, checkpoint-only validation may process the candidates
already found without archive requests. It must retain the incomplete crawl checkpoint so a
later discovery run can resume missing pages and validate only newly found or retryable items.
The CXS list API is capped at 20 results, so cutoff imports should skip unnecessary detail
requests and use only modest bounded concurrency for the detail pages that remain.
Persisted Workday detail evidence should be reused on later imports because Workday does not
provide a dependable board ETag.
Board discovery is separate from job scanning; the daily PostgreSQL path must register new
cache entries without reactivating boards an administrator disabled.
Automatically discovered boards belong in the gitignored cache and PostgreSQL; the committed
seed remains a small manual fallback rather than a generated customer directory.
Discovery summaries must account for every candidate by validation outcome and reconcile
validated-live plus retained boards with the final cache count. Refuse completion if a
validated-live board is absent from the output.

**Why:** Workday has no public board directory, and the same requisition ID can occur on
different customers. Unbounded fallback probing can also create thousands of requests, while
per-job detail enrichment can make a board import look stalled. Large shared boards can have
thousands of postings and no list-level publication dates. Without explicit accounting,
request failures look like silently discarded candidates and an incomplete run can appear
successful.

**How to apply:** Keep Workday-specific discovery, request pacing, and ID construction
inside the adapter boundary; matching, persistence, and recommendation code should remain
ATS-agnostic.