---
name: Relevance-first enrichment
description: User direction on limiting expensive job-description enrichment to plausibly matchable roles.
---

Keep a lightweight record of discovered jobs, including their direct website URL, but avoid fetching or persisting full descriptions for on-site jobs in locations where no active user could match. The user is considering role-based filtering as an additional stage after discussing location relevance.

**Why:** The user wants useful links retained without spending description-enrichment effort on jobs that cannot match active users, such as India on-site postings when no active user targets India.

**How to apply:** Use active profiles' structured location and workplace constraints before expensive detail enrichment. Preserve title, location, URL, and a reason when details are skipped. Discuss any role-based prefilter separately; keep the coverage gate aligned with whichever candidate set is ultimately selected.
