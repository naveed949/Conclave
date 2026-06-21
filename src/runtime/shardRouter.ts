import { createHash } from 'crypto';
import { AppCommand, ApplyResult, CommandMeta } from '../consensus/types';
import { MetricsRegistry } from '../platform/metrics';

/**
 * The minimal node surface the router needs: ask who leads, and submit to it.
 * Structural so any `RaftNode<C, T>` (e.g. a {@link ModuleNode}) fits without the
 * router depending on the concrete node/state-machine types.
 *
 * Note: this `{ isLeader, submit }` shape is a strict SUBSET of the `Consensus`
 * seam (ADR-0021) — the router was already decoupled from the concrete node via
 * structural typing, so any `Consensus<C, T, A>` satisfies it unchanged.
 */
export interface ShardNode<C extends AppCommand, T> {
    isLeader(): boolean;
    submit(command: C, meta?: CommandMeta): Promise<ApplyResult<T>>;
}

/**
 * Multi-Raft shard router (ADR-0020, M10).
 *
 * Each shard is an INDEPENDENT single-group Raft cluster — its own nodes, its own
 * leader election, its own replicated log + ModuleHost. The router is a thin,
 * STATE-FREE front door: it maps a command's partition key to exactly one shard
 * and submits to that shard's current leader. It holds no data of its own — it is
 * fully derivable from (a) the static shard map and (b) each group's live
 * leadership — so it can be reconstructed at any time and every participant that
 * shares the same shard count computes the same routing.
 *
 * Routing model (ADR-0020 "Sharding key"): the shard is a PURE, deterministic
 * function of the partition key — `sha256(key) mod N`. No `Date`/`Math.random`,
 * so the same key always lands on the same shard on every node and on every
 * process restart. Writes to different shards proceed in parallel (different
 * leaders), which is the write-scaling the ADR is demonstrating.
 */

/**
 * A handle to one shard's cluster. The router stays generic over the submit
 * mechanism: it is given the shard's set of nodes and asks them which one is the
 * current leader, rather than baking in transport/forwarding details. This is
 * what lets tests wire in `buildCluster(3)` per shard and `buildModuleCommand`
 * for the payload without the router knowing about either.
 */
export interface ShardHandle<C extends AppCommand = AppCommand, T = unknown> {
    /** Every node in this shard's Raft group (used to locate the current leader). */
    nodes: ShardNode<C, T>[];
}

/** Raised when a shard currently has no known leader; the caller should retry. */
export class NoShardLeaderError extends Error {
    constructor(public readonly shard: number) {
        super(`shard ${shard} has no known leader`);
        this.name = 'NoShardLeaderError';
    }
}

export class ShardRouter<C extends AppCommand = AppCommand, T = unknown> {
    private readonly shards: ShardHandle<C, T>[];

    /**
     * @param shards one handle per shard, index `i` == shard number `i`. The shard
     *   COUNT (`shards.length`) is the modulus of the routing function, so it must
     *   be identical on every participant for routing to agree.
     */
    constructor(shards: ShardHandle<C, T>[]) {
        if (shards.length === 0) {
            throw new Error('ShardRouter requires at least one shard');
        }
        this.shards = shards;
    }

    /** Number of shards (the modulus of {@link shardFor}). */
    get shardCount(): number {
        return this.shards.length;
    }

    /**
     * Deterministic shard mapping: `sha256(partitionKey) mod N`. Pure — depends
     * ONLY on the key and the shard count, never on wall-clock or randomness — so
     * every node derives the same shard for the same key (ADR-0020). The first 6
     * hex digits (24 bits) of the digest are ample entropy for a small N and keep
     * the arithmetic inside a safe integer.
     */
    shardFor(partitionKey: string): number {
        const digest = createHash('sha256').update(partitionKey).digest('hex');
        const bucket = parseInt(digest.slice(0, 6), 16);
        return bucket % this.shards.length;
    }

    /**
     * The current leader of `shard`, or `null` if none is presently known (e.g.
     * mid-election). Derived live from the group — the router caches nothing.
     */
    leaderOf(shard: number): ShardNode<C, T> | null {
        const handle = this.shards[shard];
        if (!handle) {
            throw new RangeError(`no such shard: ${shard}`);
        }
        return handle.nodes.find((n) => n.isLeader()) ?? null;
    }

    /**
     * Route a command to the leader of the shard that owns `partitionKey` and await
     * its `submit`. The leader replicates + applies as any single-group write, so
     * per-shard correctness (determinism, idempotency, audit) is unchanged. If the
     * owning shard has no known leader right now, reject with
     * {@link NoShardLeaderError} so the caller can retry across a leader change —
     * the router does no buffering of its own (ADR-0020 "Routing/membership").
     */
    submit(partitionKey: string, command: C, meta?: CommandMeta): Promise<ApplyResult<T>> {
        const shard = this.shardFor(partitionKey);
        const leader = this.leaderOf(shard);
        if (!leader) {
            return Promise.reject(new NoShardLeaderError(shard));
        }
        return leader.submit(command, meta);
    }

    /**
     * Push scrape-time shard gauges into `metrics` (Milestone 15) — the sharding
     * analog of `RaftNode.collectMetrics()`. Sets `shard_count` and, per shard,
     * `shard_has_leader{shard}` = 1 when a leader is currently known, else 0. Read
     * only (derived live from each group's leadership, the router caches nothing).
     */
    collectMetrics(metrics: MetricsRegistry): void {
        metrics.shardCount.set(this.shards.length);
        for (let i = 0; i < this.shards.length; i++) {
            metrics.shardHasLeader.set(this.leaderOf(i) ? 1 : 0, { shard: i });
        }
    }
}
