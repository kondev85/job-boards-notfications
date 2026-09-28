---
name: Job-level downstream threshold
description: The daily import rule for continuing to matching and Gemini when some active-board jobs lack descriptions.
---

Daily downstream processing is allowed when at least 96% of eligible open jobs on active boards in the run have non-empty descriptions. The denominator is jobs, not boards, and the matching pass covers all selected ATSes together. Board-fetch success retains its separate 98% threshold.

**Why:** A strict 98% job-level gate can keep a broad daily run resumable after useful enrichment is exhausted; 96% lets matching and Gemini proceed while preserving the separate board-health gate.

**How to apply:** Count active-board jobs eligible for the run's publication cutoff, allow exact 96% coverage, and keep the run resumable below that threshold. Do not let a board-failure percentage override the job-level rule.