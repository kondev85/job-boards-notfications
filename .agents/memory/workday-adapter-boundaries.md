---
name: Workday adapter boundaries
description: Durable constraints for Workday discovery and shared job identity.
---

Workday requisition IDs are only tenant- or board-scoped, so the shared external ID must
include the verified board identifier before persistence. Discovery should prefer archived
career-site paths plus live CXS verification; fallback board-name probing stays opt-in.

**Why:** Workday has no public board directory, and the same requisition ID can occur on
different customers. Unbounded fallback probing can also create thousands of requests.

**How to apply:** Keep Workday-specific discovery and ID construction inside the adapter
boundary; matching, persistence, and recommendation code should remain ATS-agnostic.