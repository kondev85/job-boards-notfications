---
name: Workable validation rate limits
description: Workable's public account jobs endpoint can impose a provider-wide rate limit during broad registry validation.
---

Workable's public `POST /api/v3/accounts/{account}/jobs` endpoint can return a provider-wide
HTTP 429 with a very long `Retry-After` after a broad multi-board sweep. A zero-result
validation under that condition is inconclusive, not evidence that the boards are invalid.

**Why:** A high-concurrency sweep triggered the same long rate limit for known-good accounts,
including an account previously confirmed to have current published jobs.

**How to apply:** Validate Workable registries conservatively with checkpointed batches,
record rate-limit failures separately from invalid feeds, and only add boards with
successful feed evidence. The accepted workflow is an append-only cache plus explicit
PostgreSQL synchronization for verified entries. Workable documents 10 account-token
requests per 10 seconds; use a 1.5-second floor, honor rate-window headers, and pause
60 seconds after 100 successes. In a sustained run, even that cadence can encounter a
provider-wide headerless 429; retry once after 60 seconds, then stop rather than loop.