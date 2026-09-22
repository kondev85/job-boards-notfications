---
name: Recommendation history windows
description: Recommendation snapshots use immutable per-run items and stable daily-run windows.
---

The recommendation publication window is independent from the import cutoff: by default it spans 30 days ending at the effective recommendation run timestamp. When a daily import run is supplied, use that run's start timestamp as the effective end so resumes do not shift the window.

**Why:** The reusable user/job review cache is intentionally mutable, while recommendation membership and weak final reviews must remain auditable across runs. A daily resume must not create a second snapshot or silently change its candidate window.

**How to apply:** Keep per-run results in the append-only history table, identify daily snapshots with the daily import run ID, and leave deterministic matches plus the one-row review cache separate.