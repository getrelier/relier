# Security Policy

## Supported versions

Relier is pre-1.0. Security fixes are released against the latest
`0.MINOR.x` line; older minor versions do not receive backports.

| Version  | Supported |
|----------|-----------|
| `0.1.x`  | ✅        |
| `< 0.1`  | ❌        |

Once `1.0.0` ships, this table will be updated to reflect a longer
support window for the most recent `1.x` line.

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Use GitHub's private vulnerability reporting:

1. Open <https://github.com/getrelier/relier/security/advisories/new>.
2. Provide a description, reproduction steps, and an assessment of
   impact (data loss, privilege escalation, denial of service, etc.).
3. If your report includes proof-of-concept code, attach it as a file
   rather than pasting into the description.

If GitHub private reporting is unavailable to you, email
`fajimikolade23@gmail.com` with `[relier-security]` in the subject
line. Include the same information as above.

## What to expect

- **Acknowledgement** within 3 business days.
- **Triage** within 7 business days we will tell you whether the
  report is accepted as a security issue or reclassified as a regular
  bug, and what the planned timeline is.
- **Coordinated disclosure** we will agree on a disclosure date
  with you. The default window is 90 days from acknowledgement, but
  high-severity issues may warrant faster disclosure and low-severity
  issues may be bundled with the next scheduled release.
- **Credit** we will credit you in the release notes and the
  GitHub Security Advisory unless you ask otherwise.

## Scope

In scope:

- The `relier` Python package (`src/relier/`).
- The bundled OpenTelemetry / Prometheus / Grafana configurations
  under `scripts/`.
- The Docker manifests (`Dockerfile`, `Dockerfile.bench`,
  `docker-compose*.yml`).
- The Lua scripts under `src/relier/storage/lua/`.

Out of scope:

- Vulnerabilities in upstream dependencies (Celery, Redis, Kombu,
  `pydantic`, OpenTelemetry SDKs). Report those to their upstream
  maintainers; we will follow up with a Relier-side mitigation once
  the upstream fix is published.
- Misconfiguration in your own deployment (e.g. exposing Redis on a
  public network with no auth). The [Configuration](https://getrelier.github.io/relier/configuration/)
  and [Deployment](https://getrelier.github.io/relier/deployment/)
  docs describe the secure defaults.

## Known sharp edges

These are deliberate trade-offs, not vulnerabilities:

- **`SECRET_KEY` default** is `"change-in-production"`. Relier surfaces
  this in `Settings`; production deploys must override
  `RELIER_SECRET_KEY`.
- **Bench / dev Grafana** anonymous viewer is enabled by default. The
  production compose manifest (`docker-compose.prod.yml`) disables it
  and requires `GRAFANA_ADMIN_PASSWORD`.
- **Task payloads in Redis** are not encrypted at rest. Run Redis with
  TLS (`rediss://`) for in-flight encryption and rely on disk-level
  encryption for at-rest. The schema envelope includes an HMAC but is
  not a confidentiality boundary.
