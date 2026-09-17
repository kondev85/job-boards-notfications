---
name: Workable validation rate limits
description: Workable's public account jobs endpoint can impose a provider-wide rate limit during broad registry validation.
---

Workable's public `POST /api/v3/accounts/{account}/jobs` endpoint can return a provider-wide
HTTP 429 with a 24-hour `Retry-After` after about 150 requests from Replit's shared outbound
network. A zero-result validation under that condition is inconclusive, not evidence that
the boards are invalid.

**Why:** Multiple paced sweeps consistently completed about 150 responses before a headerless
public-endpoint throttle returned `Retry-After: 86400`. Workable officially documents
10 requests per 10 seconds for account tokens, but that short-window limit does not explain
the additional public/shared-network quota.

**How to apply:** Validate conservatively with checkpointed batches, a default ceiling of
100 probes per 24-hour window, and the provider's longer `Retry-After` whenever present.
During an active cooldown, send no requests. Keep throttles retryable, and only register
boards with a qualifying recent posting. Do not rotate or spoof source IPs to bypass the
limit; request provider authorization or a documented partner allowance for higher volume.