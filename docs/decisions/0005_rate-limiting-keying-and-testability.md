# ADR 0005: Rate Limiting Keying and Testability

Status: Accepted

Date: 2025-09-03

## Context

The API enforces a basic per-IP rate limit (60 requests/minute). During local and CI testing, many tests share the same client IP and call a variety of endpoints in quick succession. A global per-IP counter caused unrelated tests and endpoints (e.g., `/healthz`) to intermittently hit 429 Too Many Requests, leading to flaky tests and cross-test interference.

## Decision

Change the in-memory rate limiter to key request counters by `(client_ip, path)` rather than by `client_ip` alone. This maintains a conservative per-path limit while substantially reducing interference between different endpoints during tests. The window remains 60 seconds by default.

We also preserve production behavior without requiring test-specific configuration. Dedicated rate limiting for groups of endpoints or tenants can be added later if needed.

## Consequences

- Pros:
  - Greatly improves test stability without disabling rate limiting or adding sleeps.
  - Keeps behavior deterministic and simple in production.
  - Still returns 429 for endpoint-specific bursts.
- Cons:
  - Slightly less strict than a global per-IP limit across all paths. However, for this POC, endpoint-level protection is sufficient and simpler.

## Alternatives Considered

- Skip rate limiting tests: fastest but leaves behavior unverified.
- Make window configurable via settings and shrink in tests: adds config plumbing, still does not address cross-endpoint interference.
- Token bucket / leaky bucket with shared state: more complex than needed for this POC.

## Implementation Notes

- Middleware now tracks requests in a dictionary keyed by `(client_ip, path)`.
- No changes required to endpoint code. Existing 429 responses continue to include `Retry-After` and `X-Request-ID`.


