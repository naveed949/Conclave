import { RaftNode, RaftConfig } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { PeerInfo } from '../src/consensus/types';

// Fast timers keep the test suite snappy and deterministic.
const TEST_TIMERS: Partial<RaftConfig> = {
    electionMinMs: 50,
    electionMaxMs: 100,
    heartbeatMs: 20,
};

/** Build an in-process cluster of `size` nodes wired with LocalTransport. */
export function buildCluster(size: number): RaftNode[] {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const ids = Array.from({ length: size }, (_, i) => `node${i + 1}`);

    const nodes = ids.map((id) => {
        const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
        return new RaftNode({ id, peers, ...TEST_TIMERS }, transport);
    });

    nodes.forEach((n) => registry.set(n.id, n));
    return nodes;
}

/** Poll until `predicate` is true or `timeoutMs` elapses. */
export async function waitFor(predicate: () => boolean, timeoutMs = 2000, stepMs = 10): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (predicate()) return;
        await new Promise((r) => setTimeout(r, stepMs));
    }
    throw new Error('waitFor: condition not met within timeout');
}

export function leaders(nodes: RaftNode[]): RaftNode[] {
    return nodes.filter((n) => n.isLeader());
}
