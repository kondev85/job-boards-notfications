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

Concrete work-country and city preferences are hard eligibility constraints;
broad user regions are not. For hybrid and onsite roles, the job's explicit
primary location outranks unrelated secondary offices. ATS `isRemote` flags can
be permissive even when `workplaceType` is Hybrid, so the explicit workplace
type takes precedence.

**Why:** Public ATS feeds often label US-only jobs simply as `Remote`, while
multi-office ATS address payloads and permissive remote flags can make an
ineligible office look valid if all evidence is flattened together.

**How to apply:** Match explicit job geography against each user's location
preferences before scoring. Use description text only as a cautious fallback,
prefer a concrete country/city match over broad Europe/EMEA wording, and never
treat short ISO country codes as free-text country names unless they are
country-labelled structured data.