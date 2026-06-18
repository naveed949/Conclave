# 0001. Adopt Raft consensus to decentralize the backend

- Status: Accepted
- Date: 2026-06-12

## Context

The repository described itself as "a POC of a consensus protocol… to make it
decentralized," but the implementation was a conventional centralized Express +
MongoDB book-CRUD API. There was a complete gap between the stated goal
(decentralized, consensus-based) and the code (a single-node service depending on
one database). We needed an actual mechanism for multiple peer nodes to agree on
state with no central authority.

## Decision

Implement the **Raft consensus algorithm** as the backbone of the service:
leader election plus log replication, with the application (the book service)
modeled as a replicated state machine on top of the committed log.

Raft was chosen over other consensus protocols because it is specifically
designed for understandability, has an unambiguous specification (the paper's
figure 2), and is the standard teaching/POC choice — appropriate for a project
whose *subject* is the consensus mechanism itself.

## Consequences

- The system now genuinely decentralizes: any minority of nodes can fail and the
  cluster keeps serving and re-electing leaders.
- The book domain becomes a thin state machine; consensus is the core of the
  codebase (`src/consensus/`).
- We take on the complexity of elections, replication, terms, and commit safety —
  and the obligation to test them.

## Alternatives considered

- **Paxos / Multi-Paxos** — foundational but notoriously hard to implement
  correctly and to explain; wrong fit for an understandability-focused POC.
- **A library (e.g. a Raft npm package)** — would hide the very thing the project
  exists to demonstrate; see ADR-0013.
- **Leader/follower DB replication** — still centralized around a primary; does
  not deliver leaderless fault tolerance or trustless agreement.
