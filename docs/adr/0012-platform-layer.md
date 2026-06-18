# 0012. A reusable platform layer for cross-cutting concerns

- Status: Accepted
- Date: 2026-06-18

## Context

Observability, fault tolerance, and audit are cross-cutting concerns. Implemented
per-endpoint they get duplicated, drift, and are easy to forget. The consensus
core is a generic substrate, so concerns placed there are inherited by any
application built on top.

## Decision

Group cross-cutting concerns into a dedicated `src/platform/` layer — logger,
metrics, request context/tracing, leader forwarding — and wire them into the
consensus core and the Express app as middleware/collaborators rather than into
individual handlers. The HTTP app is built by a factory (`createApp(node, deps)`)
so these are injected once and applied uniformly.

## Consequences

- Concerns are defined once and apply to every route and every app on the
  substrate; the book domain stays a thin adapter.
- Clear separation: `consensus/` (the algorithm) vs `platform/` (operational
  concerns) vs `controllers/routes` (the app).
- Testability: the app factory lets tests wire isolated nodes/deps with no global
  state.

## Alternatives considered

- **Per-controller cross-cutting code** — duplicative and error-prone.
- **A framework / DI container** — overkill for the size of this project; plain
  factories and middleware suffice.
