---
name: Remote location evidence
description: How provider geography and unknown remote scope should influence user-specific matching.
---

Structured provider geography is stronger than a generic `Remote` label. Ashby
address and secondary locations, Lever country/all-locations, and Greenhouse
location/offices/metadata should be retained when available. A generic remote
posting with no geographic evidence must not be assumed European; it may remain
eligible for a user explicitly targeting the United States, while configured
non-US users should not receive it by default.

**Why:** Public ATS feeds often label US-only jobs simply as `Remote`, while
European eligibility is frequently expressed through country, region, or
secondary-location data.

**How to apply:** Match explicit job geography against each user's location
preferences before scoring. Use description text only as a cautious fallback,
and never treat short ISO country codes as free-text country names unless they
are country-labelled structured data.