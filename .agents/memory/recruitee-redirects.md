---
name: Recruitee redirects
description: Legacy Recruitee board hosts can redirect their public offers feed to a current host.
---

Recruitee board slugs may return a 3xx redirect from `/api/offers/` to another Recruitee host. The pooled HTTP client must follow bounded redirects before parsing the feed as JSON.

**Why:** Treating the redirect body as JSON produces a misleading `JSONDecodeError`, and otherwise valid boards are recorded as failed.

**How to apply:** Keep redirect handling in the shared fetch layer so validation and imports use the same behavior; cap redirect hops to avoid loops.