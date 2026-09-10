---
name: Preview port routing
description: Avoid HTTP 426 failures caused by routing Replit preview traffic to Vite's standalone WebSocket listener.
---

Run Vite hot reload on the application’s HTTP server and map that server’s local port to external port 80. Do not expose Vite’s separate WebSocket listener as the web preview.

**Why:** A normal browser request routed to the WebSocket-only listener returns `426 Upgrade Required`, while removing that listener without correcting the external mapping can produce a 502.

**How to apply:** For Express with Vite middleware, attach HMR to the shared HTTP server and confirm the workflow reports only the application port. Verify the actual Replit development domain returns HTTP 200, not just localhost.