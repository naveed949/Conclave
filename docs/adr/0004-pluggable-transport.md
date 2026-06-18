# 0004. Pluggable transport (HTTP in prod, in-process in tests)

- Status: Accepted
- Date: 2026-06-12

## Context

Raft nodes must exchange RPCs (RequestVote, AppendEntries, later
InstallSnapshot). Real clusters need these over the network, but tests need them
to be fast and deterministic — real sockets make tests slow and flaky, and timing
assertions on elections become unreliable.

## Decision

Define a `Transport` interface and provide two implementations:

- **`HttpTransport`** — RPCs over HTTP using Node's built-in `http` module,
  matching the existing Express stack.
- **`LocalTransport`** — delivers RPCs by direct in-process method calls against a
  shared registry of nodes, with a small simulated latency.

Nodes depend only on the interface; the implementation is injected.

## Consequences

- Tests spin up multi-node clusters entirely in-process — no sockets, no DB —
  making them fast and deterministic.
- The same Raft logic runs unchanged in production over HTTP.
- A node that is "offline" in a test is modeled simply by removing it from the
  registry (the transport returns `null`).

## Alternatives considered

- **HTTP everywhere, including tests** — slower and flakier; we'd have to manage
  ports and lifecycles for every test cluster.
- **gRPC / a message bus** — more capable but adds heavy dependencies and
  infrastructure for no POC benefit (see ADR-0013).
