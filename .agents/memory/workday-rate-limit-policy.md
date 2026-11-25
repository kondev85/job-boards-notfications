---
name: Workday rate-limit policy
description: Concurrency and retry posture for Workday board-fetch throttling.
---

Keep Workday board-fetch concurrency at one and Workday detail concurrency at two until the 429 rate is re-evaluated. Use bounded exponential retry for Workday 429/5xx responses and respect `Retry-After` within the board deadline.

**Why:** Recent runs showed widespread Workday 429s, and deferred enrichment could fan out multiple detail requests per board on top of concurrent boards. A serialized board queue and small detail pool reduce request bursts while keeping other ATS workers available.

**How to apply:** Reassess concurrency only after observing 429 rates with these limits. Keep retries bounded, and do not raise Workday concurrency merely because it is the last ATS remaining.
