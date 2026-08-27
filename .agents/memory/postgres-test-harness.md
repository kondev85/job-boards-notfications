---
name: PostgreSQL test harness
description: Local PostgreSQL clusters in this Replit environment need an explicit Unix socket directory
---

Temporary PostgreSQL clusters cannot rely on the default `/run/postgresql` socket
directory in this environment; direct the server to a writable temporary socket
directory and use that path in the client DSN.

**Why:** The PostgreSQL binaries are available, but the default runtime socket
directory may not exist, causing an otherwise healthy temporary server to exit
before tests can connect.

**How to apply:** When starting an isolated local PostgreSQL test cluster, pass
`-k` with a temporary directory and connect with `host` set to that directory.