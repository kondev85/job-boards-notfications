---
name: Workday rate-limit policy
description: Concurrency and retry posture for Workday board-fetch throttling.
---

Keep Workday at two concurrent boards while other ATSes remain and cap it at three when Workday is the only ATS left. Use a longer bounded exponential retry for Workday 429/5xx responses and respect `Retry-After` within the board deadline.

**Why:** Run 49 continued to record frequent Workday 429s under the previous two-worker cap, so raising solo concurrency to four risked increasing throttling rather than improving throughput.

**How to apply:** Reassess concurrency only after observing the 429 rate under the lower cap. Keep retries bounded so a throttled board cannot stall the full daily run.
