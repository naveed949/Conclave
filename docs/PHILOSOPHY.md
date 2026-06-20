# Project Philosophy

## The problem

How do you keep a backend service **correct and available when no single machine
is in charge** — and without a central database as the source of truth?

A conventional backend has exactly one source of truth (one database). It is a
single point of failure and a single point of control: if it is down, you are
down; whoever owns it owns the data. This project asks the opposite question —
can a *cluster of equal peers* agree on every change so the system has **no
central point of authority or failure**?

## The core idea: state as a replicated log

The guiding principle, taken from Raft and the replicated–state–machine model:

> **If every node starts in the same state and applies the same commands in the
> same order, every node ends in the same state — without ever sharing storage.**

So instead of "write to the database," the system "appends a command to a log
that a majority of nodes agree on." Once a command is committed by a majority, it
is applied to each node's local state machine. Every node converges to identical
state through agreement, not through a shared store.

The demo application — a library book service (add / borrow / return) — is
deliberately incidental. It is just a **deterministic state machine** riding on
top of consensus. It could be payments, inventory, or anything else. **Consensus
is the actual subject; the books are the demo.**

## Three principles

1. **No central database = decentralization.** A shared database would
   re-centralize everything. State lives in each node's own replica.
2. **Agreement before action.** A change is not "real" until a majority of nodes
   hold it (the commit point). This is what makes it safe to lose nodes.
3. **Determinism is the contract.** All non-determinism (ids, timestamps) is
   resolved by the leader *before* a command enters the log, so replicas can
   never diverge. This is the subtle heart of the design.

## What it buys us

- **Fault tolerance** — lose a minority of nodes and the system keeps serving and
  elects new leaders.
- **No single point of failure or control** — every node is a peer; leadership
  rotates by election.
- **Consistency without a central store** — all nodes converge on identical state.

## The second idea: cross-cutting concerns belong in the substrate

Because the consensus layer is a generic platform, anything baked into it is
inherited by every application built on top. We use this to make three classic
backend concerns *built-in* rather than per-app:

- **Audit** — the replicated log already *is* an append-only, ordered record of
  every change. Making each entry hash-chained turns it into a tamper-evident
  audit trail for free.
- **Fault tolerance** — consensus already gives election + replication; we added
  durable persistence, snapshotting, idempotency, and leader forwarding around it.
- **Observability** — the consensus layer is a goldmine of signals (replication
  lag, elections, commit progress) that ordinary backends never expose.

## In one sentence

This project is a **proof-of-concept for trustless agreement among peer nodes**,
showing how to turn an ordinary centralized CRUD backend into a fault-tolerant,
decentralized, database-free system where **consensus — not a single server — is
the source of truth**, with audit, observability, and fault tolerance built into
the consensus substrate itself.

## Further reading

Design decisions are recorded as ADRs in [`docs/adr/`](./adr/README.md).
