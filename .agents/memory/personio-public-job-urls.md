---
name: Personio public job URLs
description: Personio’s public XML feed often omits the job URL even though the posting page is deterministic.
---

Personio public XML positions may include `id` and job data without a `jobUrl` or URL element. The public posting URL can be derived as `https://{board-host}/job/{id}`, preserving any provider-supplied URL when present.

**Why:** Missing URL fields caused Personio matched-job exports to lose the link even though the same posting was publicly available at the deterministic job path.

**How to apply:** Add the derived URL while parsing the feed, before normalization, so every downstream output and database upsert receives the link.