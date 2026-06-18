# 0005. Leader-only writes with follower forwarding

- Status: Accepted
- Date: 2026-06-12 (forwarding added 2026-06-18)

## Context

In Raft only the leader may append to the log, so writes must be handled by the
leader. But clients should not need to know which node is currently the leader,
and leadership changes over time.

## Decision

Writes are proposed via the leader only. A follower that receives a write
**transparently forwards** the HTTP request to the current leader and relays the
response. If no leader is currently known (or forwarding fails), it returns
`421 Misdirected Request` with a `{ leader }` hint so the client can retry. A
forwarded request is marked (`X-Forwarded-By`) so it is never forwarded twice
(loop guard).

## Consequences

- Any node accepts writes; clients can talk to any peer.
- Reads remain local (see ADR-0006), so only writes pay the forwarding hop.
- During an election (no leader) writes briefly fail with `421` — correct Raft
  behavior; clients retry.

## Alternatives considered

- **Reject non-leader writes with a redirect only** — pushes leader discovery onto
  every client; kept as the fallback, not the default.
- **Allow any node to append** — violates Raft safety; not an option.
