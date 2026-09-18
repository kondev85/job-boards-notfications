---
name: BambooHR current-openings feed
description: BambooHR’s public careers feed has active-job counts but no reliable publication timestamps.
---

BambooHR’s `/careers/list` JSON feed is a current-openings feed. A valid response with `meta.totalCount > 0` and at least one normalizable result is sufficient activity evidence for board-registry validation; individual publication dates and deadlines are not present in the current public feed.

**Why:** Confirmed active BambooHR boards return structured job summaries without `datePosted`, while stale or non-BambooHR slugs can return HTTP 200 HTML from the corporate site. Treating missing dates as invalid discarded active boards, while treating HTML as a feed caused inconclusive rows.

**How to apply:** Preserve BambooHR feed metadata during registry validation, derive `published_at` as deadline minus 30 days only when a future feed supplies a deadline, and retain valid undated current openings in bounded imports and matching. Classify successful non-JSON HTML fallbacks as invalid rather than as active or inconclusive.