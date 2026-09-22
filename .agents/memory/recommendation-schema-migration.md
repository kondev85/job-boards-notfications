---
name: Recommendation schema migration
description: Recommendation-history tables must be present in the migration source used by the web app.
---

Recommendation-history DDL must be maintained in the numbered migrations as well as the Python persistence schema initializer. The web process does not run the Python initializer or startup DDL.

**Why:** The preview can have the older recommendation tables from scraper setup while the web API fails as soon as it queries a newly added history table.

**How to apply:** Add a numbered migration, include it in the development post-merge setup, apply it to development through the supported schema flow, and rely on Publish to carry the diff to production.