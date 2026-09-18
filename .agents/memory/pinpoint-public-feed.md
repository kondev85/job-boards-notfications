---
name: Pinpoint public feed dates
description: Publication-date evidence available in Pinpoint's anonymous postings feed.
---

Pinpoint's public `postings.json` response can expose active listings without a publication timestamp. Do not infer recency from the current feed, numeric IDs, deadlines, or HTML career pages.

**Why:** The importer promises a published-after cutoff, and an active listing without a reliable date cannot prove that it was published after the cutoff.

**How to apply:** Keep the board adapter and normalization support, but exclude boards from cutoff-gated registry validation until the structured response supplies a parseable publication date or another approved public structured source does.