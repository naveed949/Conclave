# Test Coverage Analysis

_Snapshot taken 2026-06-22 on branch `claude/test-coverage-analysis-zxel4f`._

This is an assessment of where the test suite is strong, where it is thin, and a
prioritized list of concrete additions. Numbers come from
`npx jest --coverage` (Istanbul) over `src/`.

## Headline numbers

| Scope | % Stmts | % Branch | % Funcs | % Lines |
|---|---|---|---|---|
| **All files** | 85.5 | **70.6** | 87.8 | 87.3 |
| `src/consensus` | 85.5 | 72.8 | 90.2 | 87.1 |
| `src/runtime` | 92.5 | 78.1 | 96.3 | 93.2 |
| `src/edge` | 82.0 | 69.3 | 82.8 | 85.1 |
| `src/platform` | 81.5 | 56.5 | 69.7 | 83.0 |
| `src/routes` | 76.1 | 69.4 | 58.3 | 79.3 |
| `src/controllers` | 72.2 | 55.9 | 92.6 | 73.6 |
| `src` (top-level wiring) | 81.6 | 44.6 | 57.1 | 83.8 |

236 tests across 32 suites. **Statement/line coverage is healthy (~87%); branch
coverage (~70%) is the real gap** — the error and failure paths are
under-exercised, which is exactly where a consensus system earns its keep.

> Caveat on the numbers: coverage is collected with source instrumentation,
> which currently breaks the sandboxed-module suites (see finding #1). The
> `src/runtime/modules` figures (esp. `compute.ts` at 25%) are therefore
> artificially depressed — those modules are well-tested under `yarn test`, just
> not measurable under `--coverage`.

## What is already well covered

- **Consensus invariants** — `auditChain.ts` (100%), `replicatedStateMachine.ts`
  (98%), `storage.ts` (100% stmt). Snapshotting, membership/joint consensus, log
  backtracking, crash consistency, and read barriers all have dedicated suites.
- **The runtime/module framework** — determinism enforcement, merkle audit,
  effects, sharding, signing, keyed stores all have focused suites and apply-path
  convergence checks.
- **`raftNode.ts`** at 84% stmt / 89% func is solid given it is the largest and
  most intricate file in the repo.

## Prioritized gaps and proposals

### 1. Coverage instrumentation breaks the sandbox suites — and CI measures no coverage at all (highest priority)

The full suite is **green under `yarn test`** but **3 sandbox tests fail under
`--coverage`** (`tests/runtime/sandbox.test.ts`: `sumTo … converges`, the `spin`
budget test, and `admits a normal command`).

Root cause: sandboxed reducers are compiled from `fn.toString()` into a frozen
`vm` context (`src/runtime/sandbox.ts`). Istanbul rewrites function bodies to
inject counter calls, so under coverage `compute.sumTo.toString()` becomes:

```js
(state, input, ctx) => { cov_15m9dncfb6().f[1]++; const n = (cov_15m9dncfb6().s[5]++, Math.max(...)) ... }
```

`cov_15m9dncfb6` is not a safe global, so it throws `ReferenceError` inside the
sandbox → `apply()` returns 500. This means **sandboxed reducer code can never be
measured by source-level coverage**, and if coverage is ever switched on in CI
those suites go red.

Proposals:
- Exclude sandboxed modules from instrumentation (`coveragePathIgnorePatterns`
  for `src/runtime/modules/compute.ts`, or `/* istanbul ignore file */`), and add
  a regression test asserting a sandboxed reducer still applies cleanly when its
  `.toString()` contains injected identifiers (simulate by wrapping a reducer
  whose source references an out-of-scope name).
- Add a `coverageThreshold` to CI (start at the current floor, e.g. branch 70 /
  lines 85, ratcheting up) so coverage is actually enforced rather than ad-hoc.
  Today `yarn test` collects no coverage, so regressions are invisible.

### 2. Leader forwarding is essentially untested — `src/platform/forward.ts` (18% stmt, 0% branch)

`forwardToLeader()` is the entire write path for any non-leader node, yet has no
tests. None of: successful relay, upstream `Content-Type` passthrough, the
2s timeout → `false` fallback, socket-`error` → `false`, or the `X-Forwarded-By`
anti-loop guard (`isForwarded`) is covered.

Proposal: a focused suite that stands up a stub HTTP server as the "leader" and
asserts each branch (200 relay, non-JSON error body relayed verbatim, timeout,
connection refused, and that an already-forwarded request is not re-forwarded).

### 3. HTTP adapter / routes & controllers — the 4xx/5xx surface

`raftRoutes.ts` (72% stmt, **53% func**), `moduleController.ts` (67%),
`bookController.ts` (77%, 53% branch), `auditRoutes.ts` (0% branch). The happy
paths are covered via `bookApi`/`moduleApi`; the error responses largely are not:

- `NotLeaderError` → 421 and the strong-read fail-closed path
  (`?consistency=strong` on a node that can't confirm leadership).
- Malformed/oversized request bodies, missing fields, unknown ids → 400/404.
- Raft admin endpoints in `raftRoutes.ts` (lines 272–298, membership/snapshot
  triggers) that the route-level suites skip.

Proposal: extend `bookApi`/`moduleApi` (supertest) with negative-path cases per
route, and a strong-read test that asserts 421 rather than a stale local read.

### 4. `raftNode.ts` branch gaps (69% branch) — specific Raft edge cases

The uncovered lines cluster around failure handling rather than the steady state:

- **Read-barrier timeout & rejection** (`waitForApplied` 2s timeout,
  `rejectReadWaiters`, lines 732–761) — a strong read that never reaches its
  index, and a node that loses leadership mid-barrier.
- **Metrics scrape with a removed peer** (1350–1367) — the per-peer lag-gauge
  reset path after a membership shrink.
- Assorted election/replication conflict branches (335–345, 479–480, 665–666,
  934–958, 1029–1089) and the install-snapshot edges.

Proposal: targeted unit tests on a `LocalTransport` cluster for read-barrier
timeout, leadership loss during a strong read (must reject, never serve stale),
and metric correctness after a node is removed.

### 5. Edge replica SDK resilience — `src/edge` (69% branch)

`edgeReplica.ts` (79%), `httpStreamSource.ts` (75%),
`eventSourceStreamSource.ts` (80%): the reconnect/error paths
(`edgeReplica.ts` 339–386) are the thin spots.

Proposal: reconnect-after-drop resumes from the last applied index, malformed SSE
frame handling, and snapshot-then-tail resume continuity.

### 6. `src/platform` cross-cutting (57% func) and top-level wiring (45% branch)

`logger`, `metrics`, and `requestContext` are exercised incidentally but have few
direct assertions; `server.ts`/`moduleServer.ts` env parsing and bootstrap are
mostly uncovered. Lower priority (mostly glue), but the env/peer-list parsing in
the servers is worth a couple of unit tests since a bad parse is a silent
mis-configuration.

## Suggested order of work

1. Add a CI coverage step + threshold, and fix the sandbox/instrumentation
   incompatibility (#1) — without this, coverage can't even be trusted.
2. Forwarding suite (#2) — highest risk-to-effort ratio; a core path at ~0%.
3. Route/controller negative paths (#3) and `raftNode` failure branches (#4).
4. Edge resilience (#5) and platform/wiring (#6).
