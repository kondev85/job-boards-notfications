---
name: SmartRecruiters public API
description: Verified behavior of SmartRecruiters public company posting and career-page endpoints.
---

SmartRecruiters exposes public company postings at the company identifier used in
careers.smartrecruiters.com URLs. The list endpoint supports limit, offset, and
releasedAfter; list items may omit job-ad sections, so detail responses are needed to
preserve role descriptions and qualifications.

**Why:** Unknown company identifiers can return HTTP 200 with an empty content list, so
status alone is not sufficient for discovery validation. A non-empty posting result or a
career-page redirect that preserves the requested company path is required.

**How to apply:** Keep the provider behind the shared board adapter. Namespace posting IDs
with the company identifier, apply the common inclusive publication cutoff after
normalization, and exclude company-description boilerplate from role evidence.