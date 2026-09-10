---
name: Gemini recommendation sizing
description: Gemini response-size behavior for cached job review and finalist ranking.
---

Use deterministic location/workplace filters and scoring before Gemini. Send
only diverse strong deterministic fits to Gemini, with a bounded candidate cap,
and keep the final structured comparison to five jobs. Only strong final Gemini
scores should be pinned.

**Why:** Reviewing every eligible job wastes tokens and can produce detailed
evidence for obvious weak fits. Live testing also showed that 10-job and 15-job
finalist comparisons were unreliable, while five-job comparisons completed with
valid ranks and evidence.

**How to apply:** Preserve every hard-filter-eligible deterministic match, gate
Gemini behind a higher score floor, cap and diversify model candidates, retain
per-job caching, and exclude weak final reviews from pinned results. Keep the
full deterministic report available for broader calibration.