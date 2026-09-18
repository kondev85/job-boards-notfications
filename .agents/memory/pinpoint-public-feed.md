---
name: Pinpoint public feed dates
description: Publication-date evidence available in Pinpoint's anonymous postings feed.
---

Pinpoint's public `postings.json` response can expose active listings without a publication timestamp. For cutoff-gated registry validation, the approved fallback is a parseable `deadline_at` on or after the cutoff. Do not infer recency from the current feed, numeric IDs, or HTML career pages.

**Why:** The importer promises a recent active board, while Pinpoint omits publication timestamps; a future or cutoff-date deadline is the available structured evidence that the listing remains active in the requested window.

**How to apply:** Use `deadline_at` only for Pinpoint board eligibility. Keep it separate from normalized `publishedAt`, so job-search recency filters are not silently changed to use deadline dates. Some supplied slugs return an official HTML redirect to another `*.pinpointhq.com/postings.json` host; follow only that constrained target.