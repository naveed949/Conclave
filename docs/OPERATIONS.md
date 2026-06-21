# Operations

How to run, observe, and exercise the cluster as a real multi-node deployment.
This complements the local "three terminals" recipe in the
[README](../README.md) with a one-command Docker stack, metrics dashboards, and a
chaos test that drives the cluster over real HTTP.

## Run a 3-node cluster (Docker)

```bash
docker compose up --build
```

This brings up:

| Service | Host port | Notes |
|---------|-----------|-------|
| `node1` | `3001` | book-service node (leader-eligible) |
| `node2` | `3002` | book-service node |
| `node3` | `3003` | book-service node |
| `prometheus` | `9090` | scrapes each node's `/metrics` every 5s |
| `grafana` | `3000` | dashboards (login `admin` / `admin`) |

Each node is one container/process. Peers reach each other by the compose service
DNS name: every node gets `PEERS` listing the **other two** as `id@url` (self is
excluded), and `ADVERTISE_URL` set to its own service URL (e.g.
`http://node1:3001`) so membership configs carry an address peers can dial. Each
node has a named volume for its `DATA_DIR` (`/app/data`) so durable Raft state
(term, vote, log, snapshots) survives a container restart, and a `/ready`
healthcheck that turns green once the node recognises a leader.

To wipe durable state and start clean:

```bash
docker compose down -v
```

## Hit the API

Reads go to any node (local replica, eventually consistent); writes go to the
leader (a follower transparently forwards, or returns `421` if no leader is known
yet). Find the leader:

```bash
curl -s localhost:3001/raft/status   # role, term, leaderId, commit/log indices, members
```

Add a book (send to any node — it forwards to the leader):

```bash
curl -s -X POST localhost:3001/books \
  -H 'Content-Type: application/json' \
  -d '{"title":"Raft","author":"Ongaro","publisher":"Stanford","isbn":"R-1","copies":2}'
```

Read it back from a **different** node — it is already replicated:

```bash
curl -s localhost:3002/books
curl -s localhost:3003/books/<id>
```

### Strong (linearizable) reads

A default read is served from the local replica and may be slightly stale. For a
read that reflects every write committed before it, opt into the leader's
ReadIndex barrier:

```bash
curl -s 'localhost:3001/books/<id>?consistency=strong'
# or:  curl -s -H 'X-Consistency: strong' localhost:3001/books/<id>
```

The barrier confirms leadership via a fresh heartbeat quorum before serving, and
**fails closed** (`421`) if a quorum can't be confirmed — it never returns a stale
value. On a follower the read is offloaded via a ReadIndex RPC to the leader and
then served locally.

## Membership changes at runtime

Add or remove voting nodes without restarting the cluster (one change at a time;
sent to any node, forwarded to the leader):

```bash
# Add a node (start node4 with PEERS pointing at the cluster first, then):
curl -s -X POST localhost:3001/raft/members \
  -H 'Content-Type: application/json' \
  -d '{"id":"node4","url":"http://node4:3004"}'

curl -s localhost:3001/raft/members        # node4 is now a voting member

# Remove it again:
curl -s -X DELETE localhost:3001/raft/members/node4
```

## Metrics and dashboards

Each node exposes Prometheus text at `/metrics`:

```bash
curl -s localhost:3001/metrics
```

Prometheus (`deploy/prometheus.yml`) scrapes all three nodes. Every series carries
a `node="<id>"` label set by the app, so dashboards break down per node. Open
Prometheus at <http://localhost:9090> and Grafana at <http://localhost:3000>
(`admin`/`admin`); the **Raft Cluster** dashboard is auto-provisioned
(`deploy/grafana/`). Key panels and the metrics behind them:

| Panel | Metric(s) |
|-------|-----------|
| Cluster leadership | `raft_is_leader` |
| Cluster size | `raft_cluster_size` |
| Commit progress | `raft_commit_index`, `raft_last_applied` |
| Log length | `raft_log_length` |
| Elections | `raft_elections_total` |
| Replication lag (per follower) | `raft_replication_lag` (labels: `node`, `peer`) |
| HTTP throughput | `http_requests_total` (labels: `method`, `route`, `status`) |
| HTTP latency (p95) | `http_request_duration_ms` (histogram) |
| Read barriers / follower reads | `raft_read_barriers_total`, `raft_follower_reads_total` |
| State machine entries | `state_machine_entries` |
| Current term | `raft_term` |

## Chaos / fault-injection test

`tests/chaos.test.ts` spins up a real 3-node cluster over actual HTTP sockets
(real `HttpTransport`, ephemeral OS-assigned ports) — not the in-process
`LocalTransport` the other suites use — and asserts behaviour through the cluster's
HTTP API and node state, polling on real conditions (never fixed sleeps):

```bash
LOG_SILENT=true npx jest chaos
```

Each scenario maps to a real failure mode:

1. **Election** — exactly one leader emerges from cold start (maps to: bringing a
   fresh cluster online).
2. **Replicated write** — a `POST /books` to the leader commits and becomes
   readable on a *different* node (maps to: normal replication + eventual
   consistency on followers).
3. **Leader failure → recovery** — the leader's server is closed and its node
   stopped; a new leader is elected among the surviving majority and a new write
   commits through the 2-node quorum (maps to: a node crash / rolling restart).
4. **Partition tolerance** — a `PartitionableHttpTransport` isolates one node; the
   majority side keeps committing while the minority cannot, then the partition
   heals and the minority converges (maps to: a network split / one AZ
   unreachable).
5. **Strong read** — a `?consistency=strong` read against the leader exercises the
   ReadIndex barrier over real HTTP and reflects the latest write (maps to: a
   client that needs read-after-write).

The suite uses generous timers and `--detectOpenHandles --forceExit`; every node
is `node.stop()`-ed and every server `server.close()`-ed in teardown to avoid
leaked handles.
