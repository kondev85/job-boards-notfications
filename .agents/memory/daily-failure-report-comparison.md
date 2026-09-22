---
name: Daily failure report comparison
description: Daily failure snapshots are preserved by cutoff and run, with explicit repeated-failure deactivation.
---

Keep the latest failure report convenient, but preserve immutable cutoff-and-run report files for historical comparison. Compare failed board IDs across two runs before disabling; require an explicit apply action and retain database history.

**Why:** Different publication cutoffs need comparable failure snapshots, while transient provider errors must not silently deactivate active boards.

**How to apply:** Use the report exporter for dated artifacts and the board-management comparison command for a preview followed by optional deactivation.