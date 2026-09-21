---
name: Psycopg nullable parameter casts
description: PostgreSQL cannot infer types for nullable placeholders used in standalone predicates.
---

When a psycopg parameter can be `None` and appears in an expression such as `WHEN %s IS NOT NULL`, cast it explicitly to the column type in SQL.

**Why:** PostgreSQL reports `IndeterminateDatatype` for an untyped NULL placeholder even when the surrounding update logically targets a typed column.

**How to apply:** Use explicit casts for nullable timestamps, IDs, and similar values in standalone predicates or `CASE` branches; do not rely on Python-side values to provide the type.