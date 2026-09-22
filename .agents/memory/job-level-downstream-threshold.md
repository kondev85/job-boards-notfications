---
name: Job-level downstream threshold
description: The daily import rule for continuing to matching and Gemini when some active-board jobs lack descriptions.
---

Daily downstream processing is allowed when at least 98% of eligible open jobs on active boards in the run have non-empty descriptions. The denominator is jobs, not boards, and the matching pass covers all selected ATSes together.

**Why:** A small number of large or unsuitable boards can lack descriptions without making the useful majority of the daily job set unusable.

**How to apply:** Count active-board jobs eligible for the run's publication cutoff, allow exact 98% coverage, and keep the run resumable below that threshold. Do not let a board-failure percentage override the job-level rule.