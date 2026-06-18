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
    readonly raftTerm = new Gauge('raft_term', 'Current Raft term');
    readonly raftIsLeader = new Gauge('raft_is_leader', '1 if this node is the leader');
    readonly raftCommitIndex = new Gauge('raft_commit_index', 'Highest committed log index');
    readonly raftLastApplied = new Gauge('raft_last_applied', 'Highest applied log index');
    readonly raftLogLength = new Gauge('raft_log_length', 'Number of in-memory log entries (post-compaction)');
    readonly raftSnapshotIndex = new Gauge('raft_snapshot_index', 'Last log index included in the latest snapshot');
    readonly raftReplicationLag = new Gauge('raft_replication_lag', 'Entries a follower is behind the leader');
    readonly raftDedupCacheSize = new Gauge('raft_dedup_cache_size', 'Remembered requestIds in the idempotency cache (bounded)');
    readonly booksTotal = new Gauge('books_total', 'Books currently in the state machine');

    private collectors: Collector[] = [];
    private metrics = [
        this.httpRequests, this.httpDuration, this.raftElections, this.raftTerm, this.raftIsLeader,
        this.raftCommitIndex, this.raftLastApplied, this.raftLogLength, this.raftSnapshotIndex,
        this.raftReplicationLag, this.raftDedupCacheSize, this.booksTotal,
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
