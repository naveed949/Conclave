# 0006. Local, eventually-consistent reads

- Status: Accepted
- Date: 2026-06-12

## Context

Reads could be routed through the leader for linearizable (strongly consistent)
guarantees, or served locally from each node's replica for speed and
availability. Linearizable reads require extra coordination (a read-index round
trip or leader lease).

## Decision

Serve reads from the **local replica** of the node that receives them. They are
eventually consistent: a read may briefly miss the very latest committed write
that hasn't yet been applied on that particular node.

## Consequences

- Reads are fast, scale across all nodes, and stay available even on followers.
- A read immediately after a write to a *different* node may not see it yet.
- Adequate for the POC and for most read-heavy workloads.
- Linearizable reads were subsequently added as an **opt-in** (`?consistency=strong`)
  in ADR-0014; this local read remains the default.

## Alternatives considered

- **Leader-routed / read-index linearizable reads** — stronger guarantee but adds
  latency and coordination. Later adopted as an opt-in (ADR-0014) rather than the
  default.
