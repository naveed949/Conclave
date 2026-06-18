# 0013. Minimal external dependencies (implement primitives in-house)

- Status: Accepted
- Date: 2026-06-18

## Context

The project's purpose is to *demonstrate and teach* the consensus mechanism and
the surrounding backend concerns. Pulling in libraries for consensus, metrics, or
RPC would hide the very mechanics the project exists to show, and would add
operational weight to a POC.

## Decision

Implement the core primitives in-house with the standard library where reasonable:
Raft itself, the metrics registry (Prometheus text format), peer RPC (Node's
`http` module), persistence (`fs`), hashing/ids (`crypto`), and request context
(`async_hooks`). Keep runtime dependencies to the essentials already in the stack
(Express, CORS, dotenv). Stale/incorrect deps were removed (`@types/mongoose`),
and missing ones added (`@types/node`).

## Consequences

- The code is self-contained and readable; the mechanisms are visible, not hidden
  behind a package — ideal for a POC and for learning.
- Smaller dependency/attack surface and no version-churn from heavy libraries.
- We forgo battle-tested implementations; for production, swapping in mature
  libraries (prom-client, OpenTelemetry, an embedded log store, gRPC) is the
  expected upgrade path and is eased by the existing abstractions
  (`Transport`, `RaftStorage`, `MetricsRegistry`).

## Alternatives considered

- **Adopt mature libraries up front** — the right call for production, the wrong
  call for a POC whose goal is to expose the mechanics.
