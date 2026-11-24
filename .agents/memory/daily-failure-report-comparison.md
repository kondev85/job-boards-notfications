---
name: Daily failure report comparison
description: Daily failure snapshots are preserved by cutoff and run, with explicit repeated-failure deactivation.
---

Keep the latest failure report convenient, but preserve immutable cutoff-and-run report files for historical comparison. Automatically deactivate only boards whose latest completed outcome is `NotFound` on three distinct UTC run dates since the last successful outcome. Never count throttling, server errors, deadlines, or unfinished runs.

**Why:** Different publication cutoffs need comparable failure snapshots, while transient provider errors must not silently deactivate active boards. A successful later fetch means the board recovered and resets its failure count.

**How to apply:** Apply the rule after a run completes and before a new run snapshots active boards. Use the report exporter and board-management comparison command to inspect history; preserve old run records when deactivating.