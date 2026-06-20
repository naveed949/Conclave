# 0009. The replicated log as a tamper-evident audit trail

- Status: Accepted
- Date: 2026-06-18

## Context

Audit logging is usually bolted on as a separate, best-effort subsystem. But this
architecture already maintains an append-only, totally-ordered, replicated record
of every state change — the Raft log. That is, by definition, an audit trail. The
opportunity is to make audit a first-class, built-in property rather than an
afterthought.

## Decision

Treat the committed log as the audit trail. Each committed command is recorded as
an audit entry carrying `actor`, `requestId`, `timestamp`, and a **hash chained**
to the previous entry (`hash = H(prevHash + entry fields)`). Expose `GET /audit`
(filterable) and `GET /audit/verify`. Because the chain is part of the replicated
state machine, it is identical on every node.

## Consequences

- Tamper-evidence: altering any historical entry breaks the chain and is detected
  by `/audit/verify`. Forging history requires corrupting a majority of nodes.
- Audit is automatic for every write — impossible to "forget" to log a change.
- The audit log is retained in full inside snapshots (ADR-0011), so it survives
  compaction; consequently it grows unbounded — a documented POC limitation.

## Alternatives considered

- **Separate audit store / log shipping** — can drift from actual state, can be
  bypassed, and isn't inherently tamper-evident.
- **No hash chaining** — loses tamper-evidence; the chain is cheap to add and is
  the main value-add.
