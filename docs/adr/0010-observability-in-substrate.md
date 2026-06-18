# 0010. Observability built into the consensus substrate

- Status: Accepted
- Date: 2026-06-18

## Context

The original code logged with bare `console.log` and exposed no metrics or
tracing. A distributed system is especially hard to operate blind — and the
consensus layer holds signals (replication lag, elections, commit progress) that
ordinary backends never surface. Observability should be a built-in property of
the platform, not per-endpoint work.

## Decision

Add observability to the substrate (`src/platform/`):

- **Structured JSON logging** auto-tagged with node id, role/term, and the active
  request's `requestId`/`actor`.
- **Prometheus `/metrics`** (dependency-free) exposing HTTP counters/histograms
  *and* consensus gauges: `raft_is_leader`, `raft_term`, commit/applied index,
  elections, snapshot index, and per-follower `raft_replication_lag`.
- **Request-scoped tracing** via `AsyncLocalStorage`: an inbound/generated
  `X-Request-Id` propagates HTTP → log → committed command, correlating one write
  across all nodes.

## Consequences

- The cluster is debuggable and operable; replication health is directly visible.
- The same `requestId` underpins tracing, idempotency (ADR-0008), and audit
  (ADR-0009) — one identifier, three concerns.
- Every app built on the substrate inherits this for free.

## Alternatives considered

- **OpenTelemetry / prom-client / pino** — richer and standard, but add
  dependencies; for a POC we kept it in-house (see ADR-0013). These are the
  natural production upgrade path.
