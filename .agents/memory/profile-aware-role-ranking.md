---
name: Profile-aware role ranking
description: The deterministic matcher’s rule for resolving functional-depth versus title-seniority conflicts.
---

Role fit should combine explicit user role intent with the best matching experience area from the profile. When title seniority conflicts with functional depth, the role-specific experience evidence should carry more influence than an ambiguous top-level seniority label.

**Why:** A profile can have deeper experience in one function while holding higher-sounding titles in another. Treating every preferred title as an equal exact match and ranking only by visible title seniority can invert the user’s intended ranking.

**How to apply:** Use the matching experience area’s years, strength, recency, and role-specific seniority for deterministic ranking. Keep the rule generic; do not hard-code one profession as globally superior.