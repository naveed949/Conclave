import { AppCommand, ApplyResult } from './types';

/**
 * The contract an application implements to run on top of the consensus layer.
 *
 * A state machine is the application: it owns the domain state and decides what
 * each committed command does to it. The framework guarantees that every node
 * applies the same committed commands in the same order, so as long as `apply`
 * is **deterministic**, every node's state machine converges to identical state
 * without any shared/central database. This is the "decentralized" core.
 *
 * Determinism is the one hard rule (ADR-0003): `apply` must NOT read the clock,
 * generate ids/randomness, or depend on anything outside the command and the
 * current state. Resolve all non-determinism on the leader, in the command
 * builders, *before* the command enters the log.
 *
 * `snapshot`/`restore` let the framework compact the log: it periodically asks
 * the state machine to serialize its full state, discards the covered log
 * entries, and restores from the snapshot on restart or when catching up a
 * far-behind follower.
 *
 * @typeParam C - the application's command union (each a string-`type` object).
 * @typeParam T - the payload type carried back in {@link ApplyResult}.
 */
export interface StateMachine<C extends AppCommand, T = unknown> {
    /** Apply one committed command to the state. MUST be deterministic. */
    apply(command: C): ApplyResult<T>;

    /** Serialize the full state for snapshotting (must round-trip via `restore`). */
    snapshot(): unknown;

    /** Replace the state with one produced by `snapshot()`. */
    restore(data: unknown): void;

    /** Optional entity count, surfaced in node status and metrics. */
    size?(): number;
}
