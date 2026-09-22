---
name: Workday SPA detail transport
description: How public modern Workday career pages expose posting detail data.
---

Modern Workday career pages commonly return an SPA shell with no usable JSON-LD or description. The browser loads posting details from the public Calypso endpoint derived from the page URL:
`/wday/calypso/cxs/jobdetails/{tenant}/{site}/job/{location}/{posting-id}`. The page token is sent as `X-CALYPSO-CSRF-TOKEN`; retain JSON-LD as a fallback for older pages.

**Why:** The shell can be several kilobytes while containing no job description, causing a JSON-LD-only importer to mark large valid boards as permanently missing detail.

**How to apply:** Use the API response's `jobPostingInfo` fields when present, require a non-empty normalized description before treating enrichment as successful, and preserve bounded retries for throttled or empty responses.