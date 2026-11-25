---
name: Workday rate-limit policy
description: Concurrency and retry posture for Workday board-fetch throttling.
---

Keep Workday board-fetch concurrency at one and Workday detail concurrency at two until 429 rates are re-evaluated. Pace requests per tenant host, apply bounded host-specific cooldowns after 429s, and log the endpoint kind, retry number, `Retry-After`, server, and cooldown.

**Why:** Failures appeared across multiple Workday environments, with some tenant hosts failing across sibling boards. Run summaries lacked request timing and response-header evidence, so global concurrency alone could not distinguish per-tenant throttling from a shared request limit.

**How to apply:** Use tenant-host cooldowns for both CXS listing and detail requests; honor `Retry-After` up to a bounded cap. Reassess 429 rates from timestamped logs before raising concurrency or adding a global Workday limit.
