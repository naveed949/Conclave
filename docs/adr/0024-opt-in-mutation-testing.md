# 0024. Opt-in mutation testing (Stryker), dev-only

- Status: Accepted
- Date: 2026-06-22

## Context

Line/branch coverage tells us which code *ran* under test, not whether the tests
would actually *catch a regression* in it. After raising coverage to ~90% the
natural next question is test strength: a file at 100% line coverage can still
have assertions weak enough that a real bug slips through. Mutation testing
answers this by introducing small faults ("mutants") and checking the suite
fails — a killed mutant means the tests caught it.

The established tool for the TypeScript/Jest stack is **Stryker**, which is a
heavyweight dev toolchain (100+ transitive packages) and runs the suite many
times. [ADR-0013](./0013-minimal-dependencies.md) is explicit about keeping
dependencies minimal — but it scopes that rule to *runtime* primitives
(consensus, RPC, metrics, persistence), which must stay visible and in-house.

## Decision

Add Stryker as a **dev-only, opt-in** tool, not part of the default `yarn test`
or the CI gate:

- `@stryker-mutator/core` + `@stryker-mutator/jest-runner` as **devDependencies**
  (they never ship in the library surface; runtime deps are unchanged).
- `stryker.conf.json` reuses the existing `jest.config.js` and uses
  `coverageAnalysis: "perTest"` so only the tests covering a mutant run.
- A `yarn test:mutation` script. `mutate` is scoped narrowly by default (a single
  pure module) so a run is fast on a POC; widen it ad hoc to assess other code.
- `thresholds.break` is `null` — mutation score is a **diagnostic**, never a hard
  gate, so it can't make CI flaky or slow.

This honors ADR-0013's intent (no new *runtime* weight, the core stays in-house)
while gaining a sharper test-quality signal on demand.

## Consequences

- We can measure test *effectiveness*, not just reach. The initial scoped run
  (`src/runtime/canonical.ts`) scored ~94%, surfacing a few survived mutants in
  the `'drop'`-mode branch — concrete, actionable test-strengthening targets that
  coverage alone could not reveal.
- No impact on the default workflow: `yarn test`/`test:coverage` and CI are
  unchanged; mutation runs are explicit and local.
- The dev dependency footprint grows, accepted as dev-only and reversible
  (delete the two devDeps + `stryker.conf.json` + the script). Stryker output
  (`reports/`, `.stryker-tmp/`) is git-ignored.
- For production hardening, widen `mutate` to the consensus core and treat
  surviving mutants as a backlog of weak assertions to fix.
