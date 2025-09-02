# ADR 0002: Security & Hardening for POC

Status: Accepted
Date: 2025-09-02

## Context
To avoid shipping an API that is trivially open while keeping the POC lightweight, we add minimal hardening: CORS constraints, upload validation, optional API key, and a reliable healthcheck. These changes are configuration-driven and do not impact core business logic.

## Decision
- CORS: Read allowed origins from settings (`allowed_origins`), avoid wildcard `*`.
- Upload validation: Enforce extension/MIME allowlists and size limit (via settings); reject unsupported uploads.
- Auth: Optional API key via `X-API-Key` header (enabled if `api_key` is set in env). All endpoints check it.
- Healthcheck: Replace curl-based compose healthcheck with a Python one-liner to avoid dependence on extra packages.
- Dependencies: Keep `requirements.txt` as the single source of truth for this POC; `pyproject.toml` remains for future consolidation.

## Rationale
- Minimal surface change; env-driven; clear defaults.
- POC remains easy to run, but not trivially abusable.

## Consequences
- Reviewers can set `API_KEY` (mapped to `settings.api_key`) to exercise auth.
- Frontends must send `X-API-Key` when enabled and originate from allowed hosts.

## Alternatives Considered
- OAuth2/OIDC (overkill for POC).
- Keeping `*` CORS (rejected: insecure).

## Follow-ups
- Add FastAPI exception handlers and structured error model (non-blocking).
- Consider unifying dependencies in pyproject later.
