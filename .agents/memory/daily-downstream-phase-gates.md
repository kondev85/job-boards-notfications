---
name: Daily downstream phase gates
description: Daily matching and Gemini must respect ATS-specific completeness gates.
---

Daily downstream matching and Gemini may proceed once the board-success threshold passes and eligible job-description coverage reaches 94%. The board-success gate remains 98%, and the description-coverage gate is measured over eligible jobs across the selected ATSes.

**Why:** Workday detail pages can leave a large resumable backlog; waiting for every description can block useful matching and recommendations after enrichment is effectively exhausted.

**How to apply:** Count eligible jobs, allow exact 94% coverage, and keep the run resumable below that threshold. Preserve idempotent matching, missing-description resume queries, and daily heartbeats around matching, enrichment, and recommendation batches.