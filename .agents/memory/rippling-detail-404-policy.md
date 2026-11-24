---
name: Rippling detail 404 policy
description: How to handle stale individual job-detail URLs returned by Rippling listings.
---

If a Rippling listing succeeds but an individual posting-detail request returns 404, keep the listing item and continue the board. Log the number of missing details and a bounded sample of page/posting IDs. Other failures still fail the board.

**Why:** A stale detail URL should not turn an otherwise usable board listing into a board-level failure; list data remains useful but may omit detail fields.

**How to apply:** Keep detail 404 handling local to the individual posting request. Do not suppress a 404 from the board-list endpoint or other request errors.
