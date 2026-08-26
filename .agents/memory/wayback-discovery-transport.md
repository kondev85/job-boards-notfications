---
name: Wayback discovery transport
description: External archive transport behavior observed during full Ashby discovery
---

Full Ashby board discovery can time out when the scraper's Wayback CDX URL is accessed over HTTP, while the equivalent HTTPS endpoint returns the archive response successfully in the Replit environment.

**Why:** A full refresh otherwise appears stuck before candidate extraction even though the archive service is reachable.

**How to apply:** If a refresh stalls at the Wayback query, test the HTTPS endpoint before changing scraper logic or assuming the ATS APIs are unavailable.