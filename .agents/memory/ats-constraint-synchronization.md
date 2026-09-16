---
name: ATS constraint synchronization
description: The project has separate TypeScript startup and Python persistence schema definitions for ATS allowlists.
---

The allowed ATS values must stay synchronized in both the TypeScript startup
migration and the Python persistence schema. Startup migrations must include
every ATS already present in `job_boards` and `jobs` before recreating their
check constraints.

**Why:** Reapplying a stale narrower check constraint drops the working
constraint and then fails against existing SmartRecruiters or Workday rows,
preventing the web app from starting.

**How to apply:** When adding or removing an ATS, update both constraint
definitions and run the startup migration against data containing all existing
ATS values before restarting the app.