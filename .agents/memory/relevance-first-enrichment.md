---
name: Relevance-first enrichment
description: User direction on limiting expensive job-description enrichment to plausibly matchable roles.
---

Keep a lightweight record of discovered jobs, including their direct website URL, but avoid fetching or persisting full descriptions for jobs that cannot match an active user. Apply workplace/location checks first, then a cautious role check before detail enrichment.

**Why:** The user wants useful links retained without spending description-enrichment effort on jobs that cannot match active users, such as India on-site postings when no active user targets India.

**How to apply:** Use active profiles' structured location/workplace constraints before a role prefilter based on listing metadata. Preserve title, location, and URL when skipping descriptions. Keep the coverage gate aligned with the same eligible candidate set.
