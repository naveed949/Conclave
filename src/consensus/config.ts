import { PeerInfo } from './types';

/**
 * Environment-derived node options: everything needed to configure a node except
 * the application state machine and the runtime collaborators (logger, metrics,
 * storage, transport), which the process entry point supplies. Spread these into
 * a {@link RaftConfig} alongside a `stateMachine`.
 */
export interface NodeEnvOptions {
    id: string;
    peers: PeerInfo[];
    selfUrl?: string;
    electionMinMs?: number;
    electionMaxMs?: number;
    heartbeatMs?: number;
    snapshotThreshold?: number;
    dedupLimit?: number;
}

/**
 * Build a node's environment-derived options from environment variables.
 *
 *   NODE_ID         unique id for this node           (default: "node1")
 *   PORT            HTTP port this node listens on     (default: 3000)
 *   PEERS           OTHER nodes as "id@url" CSV        (default: "")
 *                   e.g. "node2@http://localhost:3002,node3@http://localhost:3003"
 *   ELECTION_MIN_MS / ELECTION_MAX_MS / HEARTBEAT_MS   timer tuning (optional)
 *   RAFT_DEBUG      "true" to log role changes         (default: false)
 */
export function loadRaftConfig(env: NodeJS.ProcessEnv = process.env): NodeEnvOptions {
    const id = env.NODE_ID || 'node1';
    const peers: PeerInfo[] = (env.PEERS || '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .map((entry) => {
            const [peerId, url] = entry.split('@');
            if (!peerId || !url) {
                throw new Error(`Invalid PEERS entry "${entry}" — expected "id@url"`);
            }
            return { id: peerId, url };
        });

    return {
        id,
        peers,
        // Address other nodes use to reach this one (advertised in membership configs).
        selfUrl: env.ADVERTISE_URL || `http://localhost:${getPort(env)}`,
        electionMinMs: env.ELECTION_MIN_MS ? Number(env.ELECTION_MIN_MS) : undefined,
        electionMaxMs: env.ELECTION_MAX_MS ? Number(env.ELECTION_MAX_MS) : undefined,
        heartbeatMs: env.HEARTBEAT_MS ? Number(env.HEARTBEAT_MS) : undefined,
        snapshotThreshold: env.SNAPSHOT_THRESHOLD ? Number(env.SNAPSHOT_THRESHOLD) : undefined,
        dedupLimit: env.DEDUP_LIMIT ? Number(env.DEDUP_LIMIT) : undefined,
    };
}

export function getPort(env: NodeJS.ProcessEnv = process.env): number {
    return env.PORT ? Number(env.PORT) : 3000;
}
