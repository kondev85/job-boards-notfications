---
name: Workable location state
description: Workable widget payloads can use state for geographic regions.
---

Do not interpret a non-empty Workable `state` field as a job publication status. In widget rows, values such as Catalonia and Masovian Voivodeship are location regions, while the posting is already visible and published through the public career site.

**Why:** Treating `state != published` as closed discarded every AirHelp posting with a regional location, leaving the board active but with zero stored jobs.

**How to apply:** Trust the public widget row when it has a title and posting identifier. Use `published_on`/`published`/`publishedAt` for date filtering, and keep location state inside location normalization.