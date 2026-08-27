---
name: Greenhouse discovery quirks
description: Full Greenhouse Wayback discovery and content imports can diverge because HEAD validation may pass while GET requests later return 406.
---

Full Greenhouse discovery can produce archive-derived candidates whose posting API
`HEAD` request returns success but whose normal job `GET` returns HTTP 406. A large
HEAD validation sweep can also be followed by 406 responses from known-good boards,
so a validation result is not sufficient permission to immediately launch a
content-enriched import.

**Why:** The project previously used lightweight HEAD probes to avoid downloading
large Greenhouse payloads, but the API behavior changed under high request volume
and made the first content batch fail uniformly.

**How to apply:** Keep archive discovery, HEAD validation, and content import as
separate phases. Pause for cooldown after a large validation pass, use small
content batches, and treat 406 as a retriable service/traffic condition rather
than proof that a board is permanently dead.