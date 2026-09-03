---
name: Gemini recommendation sizing
description: Gemini response-size behavior for cached job review and finalist ranking.
---

Use Gemini batch reviews to score the full candidate pool, but keep the final
structured comparison to five jobs. Larger finalist responses can return
malformed JSON even when response schema enforcement is enabled; strict local
validation should reject those responses rather than guessing.

**Why:** Live testing showed that 10-job and 15-job finalist comparisons were
unreliable, while five-job comparisons completed with valid ranks and evidence.

**How to apply:** Preserve the score-floor filter and per-batch cache, then
select a diverse five-job finalist set for the final ranked recommendation
report. Keep the full deterministic report available for broader calibration.