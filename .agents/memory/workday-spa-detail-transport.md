---
name: Workday detail policy
description: Project policy for Workday postings whose public detail pages omit descriptions.
---

Do not use Workday's public Calypso job-details endpoint for broad enrichment. It is hard-throttled and is not a reliable daily-import transport. When a board produces many jobs without descriptions, review its exact slug and company/board identifier and disable the board if it is not worth scanning.

**Why:** Broad detail requests can be throttled, while a small number of high-volume boards can dominate the daily backlog and prevent useful downstream completion.

**How to apply:** Keep board-level missing-description reports, preserve resumable daily phases, and make active-board changes explicitly after review rather than retrying the throttled public detail endpoint.