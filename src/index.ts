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
