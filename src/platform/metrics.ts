// A tiny, dependency-free Prometheus-compatible metrics registry.

type Labels = Record<string, string | number>;

function labelKey(labels?: Labels): string {
    if (!labels) return '';
    return Object.keys(labels)
        .sort()
        .map((k) => `${k}="${String(labels[k]).replace(/"/g, '\\"')}"`)
        .join(',');
}

function withBraces(key: string): string {
    return key ? `{${key}}` : '';
}

class Counter {
    private series = new Map<string, { labels?: Labels; value: number }>();
    constructor(readonly name: string, readonly help: string) {}

    inc(labels?: Labels, by = 1): void {
        const k = labelKey(labels);
        const e = this.series.get(k);
        if (e) e.value += by;
        else this.series.set(k, { labels, value: by });
    }

    expose(): string {
        const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} counter`];
        for (const [k, s] of this.series) lines.push(`${this.name}${withBraces(k)} ${s.value}`);
        return lines.join('\n');
    }
}

class Gauge {
    private series = new Map<string, { labels?: Labels; value: number }>();
    constructor(readonly name: string, readonly help: string) {}

    set(value: number, labels?: Labels): void {
        this.series.set(labelKey(labels), { labels, value });
    }

    /** Drop a single labelled series (e.g. a peer that left the cluster). */
    remove(labels?: Labels): void {
        this.series.delete(labelKey(labels));
    }

    /** Drop all series (re-populated by the collector at scrape time). */
    reset(): void {
        this.series.clear();
    }

    expose(): string {
        const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} gauge`];
        for (const [k, s] of this.series) lines.push(`${this.name}${withBraces(k)} ${s.value}`);
        return lines.join('\n');
    }
}

class Histogram {
    private buckets: number[];
    private counts: Map<string, number[]> = new Map();
    private sums = new Map<string, number>();
    private totals = new Map<string, number>();
    constructor(readonly name: string, readonly help: string, buckets: number[]) {
        this.buckets = [...buckets].sort((a, b) => a - b);
    }

    observe(value: number, labels?: Labels): void {
        const k = labelKey(labels);
        if (!this.counts.has(k)) {
            this.counts.set(k, new Array(this.buckets.length).fill(0));
            this.sums.set(k, 0);
            this.totals.set(k, 0);
        }
        const c = this.counts.get(k)!;
        this.buckets.forEach((b, i) => { if (value <= b) c[i] += 1; });
        this.sums.set(k, this.sums.get(k)! + value);
        this.totals.set(k, this.totals.get(k)! + 1);
    }

    expose(): string {
        const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} histogram`];
        for (const [k, c] of this.counts) {
            const base = k ? `${k},` : '';
            this.buckets.forEach((b, i) => {
                lines.push(`${this.name}_bucket{${base}le="${b}"} ${c[i]}`);
            });
            lines.push(`${this.name}_bucket{${base}le="+Inf"} ${this.totals.get(k)}`);
            lines.push(`${this.name}_sum${withBraces(k)} ${this.sums.get(k)}`);
            lines.push(`${this.name}_count${withBraces(k)} ${this.totals.get(k)}`);
        }
        return lines.join('\n');
    }
}

/** Collectors are run at scrape time so gauges reflect live state (e.g. Raft). */
type Collector = () => void;

export class MetricsRegistry {
    readonly httpRequests = new Counter('http_requests_total', 'Total HTTP requests');
    readonly httpDuration = new Histogram(
        'http_request_duration_ms',
        'HTTP request duration in milliseconds',
        [1, 5, 10, 25, 50, 100, 250, 500, 1000],
    );
    readonly raftElections = new Counter('raft_elections_total', 'Elections this node has started');
    readonly raftReadBarriers = new Counter('raft_read_barriers_total', 'Linearizable read barriers served as leader');
    readonly raftFollowerReads = new Counter('raft_follower_reads_total', 'Linearizable reads served locally on a follower via a ReadIndex from the leader');
    readonly raftTerm = new Gauge('raft_term', 'Current Raft term');
    readonly raftIsLeader = new Gauge('raft_is_leader', '1 if this node is the leader');
    readonly raftCommitIndex = new Gauge('raft_commit_index', 'Highest committed log index');
    readonly raftLastApplied = new Gauge('raft_last_applied', 'Highest applied log index');
    readonly raftLogLength = new Gauge('raft_log_length', 'Number of in-memory log entries (post-compaction)');
    readonly raftSnapshotIndex = new Gauge('raft_snapshot_index', 'Last log index included in the latest snapshot');
    readonly raftReplicationLag = new Gauge('raft_replication_lag', 'Entries a follower is behind the leader');
    readonly raftDedupCacheSize = new Gauge('raft_dedup_cache_size', 'Remembered requestIds in the idempotency cache (bounded)');
    readonly raftClusterSize = new Gauge('raft_cluster_size', 'Voting members in the current cluster configuration');
    readonly raftStreamSubscribers = new Gauge('raft_stream_subscribers', 'Active committed-log read-stream subscribers (edge replicas) on this node');
    readonly stateMachineEntries = new Gauge('state_machine_entries', 'Entries currently in the application state machine');

    // --- Module runtime observability (Milestone 15, ADR-0019). These mirror the
    // Raft gauges above but for the module runtime: command throughput, outbox/audit
    // depth, effect execution, and shard leadership. Per-node, like the Raft series.
    readonly moduleCommands = new Counter('module_commands_total', 'Module commands applied, by module/command/status');
    readonly moduleCommandDuration = new Histogram(
        'module_command_duration_ms',
        'Module command host-apply duration in milliseconds',
        [0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000],
    );
    readonly moduleOutboxPending = new Gauge('module_outbox_pending', 'Outbox effect intents still pending execution');
    readonly moduleOutboxDone = new Gauge('module_outbox_done', 'Outbox effect intents already executed (done)');
    readonly moduleAuditSize = new Gauge('module_audit_size', 'Audited module commands (Merkle audit leaf count)');
    readonly moduleRegistered = new Gauge('module_registered', 'Modules registered in the runtime host');
    readonly effectRuns = new Counter('effect_runs_total', 'Effect handler invocations, by kind and outcome');
    readonly shardHasLeader = new Gauge('shard_has_leader', '1 if the shard currently has a known leader, else 0');
    readonly shardCount = new Gauge('shard_count', 'Number of shards in the router');

    private collectors: Collector[] = [];
    private metrics = [
        this.httpRequests, this.httpDuration, this.raftElections, this.raftReadBarriers, this.raftFollowerReads, this.raftTerm,
        this.raftIsLeader, this.raftCommitIndex, this.raftLastApplied, this.raftLogLength,
        this.raftSnapshotIndex, this.raftReplicationLag, this.raftDedupCacheSize, this.raftClusterSize,
        this.raftStreamSubscribers, this.stateMachineEntries,
        this.moduleCommands, this.moduleCommandDuration, this.moduleOutboxPending, this.moduleOutboxDone,
        this.moduleAuditSize, this.moduleRegistered, this.effectRuns, this.shardHasLeader, this.shardCount,
    ];

    registerCollector(c: Collector): void {
        this.collectors.push(c);
    }

    expose(): string {
        for (const c of this.collectors) c();
        return this.metrics.map((m) => m.expose()).join('\n\n') + '\n';
    }
}

/** Shared process-wide registry. */
export const metrics = new MetricsRegistry();
