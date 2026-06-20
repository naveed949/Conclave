import http from 'http';
import { URL } from 'url';
import {
    AppendEntriesArgs,
    AppendEntriesReply,
    InstallSnapshotArgs,
    InstallSnapshotReply,
    PeerInfo,
    RequestVoteArgs,
    RequestVoteReply,
} from './types';

/**
 * Abstraction over how a node reaches its peers. Decoupling this lets us run
 * a real network cluster (HttpTransport) in production and a deterministic,
 * in-process cluster (LocalTransport) in tests without any sockets.
 */
export interface Transport {
    sendRequestVote(peer: PeerInfo, args: RequestVoteArgs): Promise<RequestVoteReply | null>;
    sendAppendEntries(peer: PeerInfo, args: AppendEntriesArgs): Promise<AppendEntriesReply | null>;
    sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs): Promise<InstallSnapshotReply | null>;
}

/** What a node must expose so transports can deliver RPCs to it. */
export interface RpcHandler {
    handleRequestVote(args: RequestVoteArgs): RequestVoteReply;
    handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply;
    handleInstallSnapshot(args: InstallSnapshotArgs): InstallSnapshotReply;
}

/** Minimal JSON POST over Node's http module (no external deps). */
function postJson<T>(url: string, body: unknown, timeoutMs: number): Promise<T> {
    return new Promise((resolve, reject) => {
        const target = new URL(url);
        const payload = JSON.stringify(body);
        const req = http.request(
            {
                hostname: target.hostname,
                port: target.port,
                path: target.pathname,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(payload),
                },
                timeout: timeoutMs,
            },
            (res) => {
                let data = '';
                res.on('data', (chunk) => (data += chunk));
                res.on('end', () => {
                    // A non-2xx or empty/non-JSON body (e.g. a peer crashing mid-request)
                    // is treated as "no reply" by callers, which map a rejection to null.
                    if (!res.statusCode || res.statusCode >= 300 || data.length === 0) {
                        reject(new Error(`bad response: status=${res.statusCode} len=${data.length}`));
                        return;
                    }
                    try {
                        resolve(JSON.parse(data) as T);
                    } catch (err) {
                        reject(err);
                    }
                });
            },
        );
        req.on('error', reject);
        req.on('timeout', () => req.destroy(new Error('request timeout')));
        req.write(payload);
        req.end();
    });
}

/** Talks to peers over HTTP. Network errors resolve to null (treated as "no reply"). */
export class HttpTransport implements Transport {
    constructor(private readonly rpcTimeoutMs = 100) {}

    async sendRequestVote(peer: PeerInfo, args: RequestVoteArgs): Promise<RequestVoteReply | null> {
        try {
            return await postJson<RequestVoteReply>(`${peer.url}/raft/request-vote`, args, this.rpcTimeoutMs);
        } catch {
            return null;
        }
    }

    async sendAppendEntries(peer: PeerInfo, args: AppendEntriesArgs): Promise<AppendEntriesReply | null> {
        try {
            return await postJson<AppendEntriesReply>(`${peer.url}/raft/append-entries`, args, this.rpcTimeoutMs);
        } catch {
            return null;
        }
    }

    async sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs): Promise<InstallSnapshotReply | null> {
        try {
            // Snapshots can be large; allow more time than a heartbeat RPC.
            return await postJson<InstallSnapshotReply>(`${peer.url}/raft/install-snapshot`, args, this.rpcTimeoutMs * 10);
        } catch {
            return null;
        }
    }
}

/**
 * In-process transport for tests: delivers RPCs by direct method call against
 * a shared registry of nodes. Optional latency simulates the network and
 * keeps elections from resolving in a single synchronous tick.
 */
export class LocalTransport implements Transport {
    constructor(
        private readonly registry: Map<string, RpcHandler>,
        private readonly latencyMs = 1,
    ) {}

    private async deliver<T>(peerId: string, fn: (h: RpcHandler) => T): Promise<T | null> {
        const handler = this.registry.get(peerId);
        if (!handler) return null; // peer is "down"
        await new Promise((r) => setTimeout(r, this.latencyMs));
        return fn(handler);
    }

    sendRequestVote(peer: PeerInfo, args: RequestVoteArgs): Promise<RequestVoteReply | null> {
        return this.deliver(peer.id, (h) => h.handleRequestVote(args));
    }

    sendAppendEntries(peer: PeerInfo, args: AppendEntriesArgs): Promise<AppendEntriesReply | null> {
        return this.deliver(peer.id, (h) => h.handleAppendEntries(args));
    }

    sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs): Promise<InstallSnapshotReply | null> {
        return this.deliver(peer.id, (h) => h.handleInstallSnapshot(args));
    }
}
