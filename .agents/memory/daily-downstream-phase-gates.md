---
name: Daily downstream phase gates
description: Daily matching and Gemini must respect ATS-specific completeness gates.
---

Run deterministic matching for non-Workday ATS jobs after the board-success threshold, while Workday detail enrichment continues. Match Workday only after the current run has no eligible missing descriptions, and run Gemini only after both matching phases and the full board snapshot are complete.

**Why:** Workday detail pages can create a large resumable backlog, but delaying all matching wastes reliable non-Workday results. Gemini recommendations are more sensitive to incomplete descriptions than deterministic matching.

**How to apply:** Preserve idempotent matching, missing-description resume queries, and daily heartbeats around matching, enrichment, and recommendation batches.