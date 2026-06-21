// Public entry point for the consensus framework (embedded-library use).
//
// Import the core, implement a `StateMachine` for your domain, build commands
// on the leader, and wire a `RaftNode` with a `Transport` and `RaftStorage`.
// The book service under `models/`, `controllers/`, and `app.ts` is a worked
// example of exactly this — not part of the framework.

// Consensus core
export { RaftNode, NotLeaderError, MembershipError } from './consensus/raftNode';
export type { RaftConfig } from './consensus/raftNode';
export type { StateMachine } from './consensus/stateMachine';
export { ReplicatedStateMachine, DEFAULT_DEDUP_LIMIT } from './consensus/replicatedStateMachine';
export type { RsmSnapshot } from './consensus/replicatedStateMachine';

// Transport + storage seams (swap implementations for production)
export { HttpTransport, LocalTransport } from './consensus/transport';
export type { Transport, RpcHandler } from './consensus/transport';
export { FileStorage, MemoryStorage } from './consensus/storage';
export type { RaftStorage, PersistentState } from './consensus/storage';

// Configuration helpers
export { loadRaftConfig, getPort } from './consensus/config';
export type { NodeEnvOptions } from './consensus/config';

// Core types an application touches
export type {
    AppCommand,
    Command,
    CommandMeta,
    ApplyResult,
    LogEntry,
    AuditEntry,
    PeerInfo,
    Role,
    Snapshot,
} from './consensus/types';
export { isControlCommand, isConfigCommand } from './consensus/types';

// Platform (observability + HTTP helpers usable by any application)
export { MetricsRegistry, metrics } from './platform/metrics';
export { createLogger } from './platform/logger';
export type { Logger } from './platform/logger';

// ---- Module runtime (ADR-0019): an opinionated application you can plug in via
// the StateMachine seam above, instead of writing one by hand. Write business
// logic as pure module reducers and inherit determinism enforcement, an
// exactly-once effect outbox, a Merkle audit, signed commands, a keyed store,
// CQRS read projections, and cross-shard sagas. See src/runtime/README.md.
export { ModuleStateMachine } from './runtime/moduleStateMachine';
export { ModuleHost } from './runtime/moduleHost';
export { defineModule } from './runtime/defineModule';
export { defineKeyedModule } from './runtime/keyedModule';
export type { KeyedModuleDefinition, KeyedReducer } from './runtime/keyedModule';
export { buildModuleCommand, buildSignedModuleCommand } from './runtime/command';
export { resolveSeed, createContext } from './runtime/context';
export { EffectExecutor } from './runtime/effectExecutor';
export { defineProjection } from './runtime/projection';
export { ProjectionHost } from './runtime/projectionHost';
export { MemoryStateStore, StoreView } from './runtime/stateStore';
export { MerkleAudit, EMPTY_ROOT } from './runtime/merkleAudit';
export { generateActorKeypair, signCommand, verifyCommand, KeyRegistry } from './runtime/signing';
export { ShardRouter, NoShardLeaderError } from './runtime/shardRouter';
export { runSaga } from './runtime/saga';
export type {
    ModuleDefinition,
    ModuleAppCommand,
    ModuleCommand,
    ModuleApplyResult,
    Reducer,
    ReducerContext,
    EffectIntent,
    EffectHandler,
    Seed,
} from './runtime/types';
export type { StateStore } from './runtime/stateStore';
export type { ProjectionDefinition, ProjectionEvent } from './runtime/projection';
export type { SagaStep, SagaResult } from './runtime/saga';
export type { ShardHandle, ShardNode } from './runtime/shardRouter';
