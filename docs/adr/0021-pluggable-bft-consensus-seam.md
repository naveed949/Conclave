# 0021. Pluggable BFT consensus — the seam, and why it is not built

- Status: Proposed
- Date: 2026-06-21

## Context

ADR-0018 established that this framework is **Crash-Fault-Tolerant (CFT)**, not
**Byzantine-Fault-Tolerant (BFT)**: Raft assumes peers are honest-but-may-crash.
ADR-0019 (pillar 7) listed an "optional BFT consensus swap behind the existing
consensus seam" as a way to extend the system to **multi-party trustlessness**
(mutually-distrusting operators) when a single trust domain is not enough. M7
(signed commands) already added per-actor accountability, but it does not make
the *ordering* Byzantine-tolerant — a malicious leader can still equivocate
(propose different logs to different followers) or censor, and CFT consensus does
not defend against that.

This ADR honestly assesses what a BFT swap would actually require on this
codebase, and records the decision **not** to build it in the prototype.

## The honest state of the "seam"

ADR-0019 implies BFT could drop in "behind the existing consensus seam." On
inspection that is optimistic. The current seam is the `Transport` interface
(`src/consensus/transport.ts`), whose methods are **Raft-specific**:
`sendRequestVote`, `sendAppendEntries`, `sendInstallSnapshot`. A BFT protocol
(PBFT, Tendermint) does not use those messages at all — it uses a different,
multi-phase exchange (e.g. pre-prepare → prepare → commit, plus view-change),
needs `3f+1` members to tolerate `f` Byzantine faults (vs Raft's `2f+1` for `f`
crashes), and requires every consensus message to be **signed** and cross-checked
so no participant can be taken at its word.

So the real seam is not `Transport` but the boundary *above the log*: the
`RaftNode` API the rest of the system depends on — `submit(command) → committed,
ordered, applied result`, plus leadership/term/membership status. Everything in
`src/runtime/` (M1–M10) and the HTTP layer depends only on that
commit-ordered-log contract, **not** on Raft internals. That contract is the true
pluggability point.

## Decision

**Do not implement BFT in the prototype.** Instead:

1. **Name the real seam.** Define (in documentation, and as a refactoring target)
   a `Consensus` interface capturing exactly what the runtime needs: `submit`,
   `readBarrier`, membership, and a leadership/status view — the subset of
   `RaftNode` that `ReplicatedStateMachine` and the controllers actually use. A
   BFT engine would implement *that*, not the Raft `Transport`.
2. **Reuse M7 as a building block.** BFT requires signed, non-repudiable messages;
   the ed25519 signing + `KeyRegistry` from M7 is the cryptographic primitive a
   BFT layer would build on (signing *consensus votes*, not just client commands).
3. **Keep determinism as the invariant that ports unchanged.** The deterministic
   state machine, the Merkle audit (M3), the canonical serialization (M5), and the
   leader-resolved-seed discipline are all consensus-agnostic — they work
   identically under BFT, because they only assume a committed, totally-ordered
   command log. This is the payoff of the core/edge split (ADR-0019): the *edge*
   and *core* do not change when the *ordering* protocol does.
4. **Treat BFT as a distinct project** gated on a real multi-party requirement,
   not a milestone of this prototype.

## Consequences

### Positive

- Honest scoping: we do not ship a fake or half-correct BFT (a subtly-wrong BFT is
  worse than none — it gives false trust-minimization guarantees).
- The work that *does* port — determinism, audit, signing, the core/edge split —
  is already done and validated, so a future BFT effort starts from a clean
  log-contract boundary rather than a rewrite of the runtime.
- Documents a real correction to ADR-0019's "swap behind the existing seam"
  framing, so future readers aren't misled into thinking it's a `Transport` swap.

### Negative

- The system remains single-trust-domain (CFT) — unsuitable for adversarial,
  multi-organization deployments, exactly as ADR-0018 concluded.
- The application boundary *below* the log — the `StateMachine` interface — is now
  extracted (ADR-0017), so applications already plug in cleanly. But the ordering
  boundary *above* the log (a `Consensus` interface a BFT engine would implement)
  is still not extracted: `RaftNode` is referenced directly (e.g. `ShardRouter`,
  controllers), so a real BFT swap would first require that second refactor.

## Alternatives considered

- **Build a minimal PBFT/Tendermint engine now.** Rejected: thousands of lines,
  a different protocol with view-changes and `3f+1` quorums, and high risk of
  subtle safety bugs — not a prototype increment, and dangerous to ship half-done.
- **Claim the `Transport` interface is the BFT seam (as ADR-0019 implied).**
  Rejected as inaccurate: BFT does not use Raft's RPCs; pretending otherwise would
  mislead.
- **Adopt an external BFT platform (Tendermint/CometBFT, HotStuff) and run the
  deterministic state machine on top via ABCI.** The most realistic path to actual
  BFT — the state machine, audit, and signing port directly onto an ABCI-style
  app interface. Recorded as the recommended direction *if* multi-party
  trustlessness is ever required; deliberately out of scope here.
