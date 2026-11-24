---
name: Relevance-first enrichment
description: User direction on limiting expensive job-description enrichment to plausibly matchable roles.
---

The user wants to avoid enriching on-site jobs in a country when no active user there could match, rather than fetching descriptions for every job regardless of the active profiles.

**Why:** The user pointed to India on-site postings as unnecessary description work when there are no active users targeting India.

**How to apply:** Use active profiles' structured location and workplace constraints before expensive detail enrichment. Keep the coverage gate's denominator aligned with the same eligible set.
