# Edge read replica — worked example (ADR-0023)

A **read-only, non-voting replica** of the library state machine that runs *outside
the cluster* — in a browser tab or a Node process — by tailing a node's
committed-log stream (`GET /raft/stream`). Reads are answered from a local copy
with no network round-trip, and the view updates live as new writes commit.

This demonstrates [ADR-0023](../../docs/adr/0023-edge-read-replicas-in-the-browser.md),
built on the M20 stream endpoint and the M21 `EdgeReplica` SDK (`src/edge/`).

## How it works

```
        writes (POST /books)                 reads (local, no round-trip)
  client ───────────────────► leader              ▲
                                 │ replicate        │ apply committed commands
                                 ▼                  │
  any node's committed log ──── GET /raft/stream ──► EdgeReplica (browser / Node)
        (snapshot handoff → live tail of committed entries; SSE)
```

- **Writes** still go through the authenticated leader path (the node forwards to
  the leader). The replica never votes, acks, or writes — it is a learner.
- **The stream is served from any node** (leader *or* follower): committed reads
  are local and eventually consistent (ADR-0006), so read serving fans out.
- **Bootstrap → tail:** a fresh consumer gets a `snapshot` (if the log was
  compacted), replays the committed tail as `entry` events, then live-tails.

## Run a node

```bash
yarn build
NODE_ID=node1 PORT=3001 node dist/server.js
# …or bring up the 3-node cluster: docker compose up  (node1 → http://localhost:3001)
```

CORS is enabled on the node, so a browser on any origin can read the stream.

## Browser demo (zero build)

Open `index.html` (serve the folder over http so ES modules load):

```bash
# from the repo root, after a node is running on :3001
npx --yes http-server examples/edge-replica -p 8080   # or: python3 -m http.server 8080 -d examples/edge-replica
# then visit http://localhost:8080/?node=http://localhost:3001
```

Add a book in the form (a write → forwarded to the leader) and watch the row
appear in the list **with no refresh** — it streamed back to the in-page replica.
Open the page in two tabs to see both converge.

> The browser reducer in `app.js` is a hand-port of the server's
> `BookStateMachine.apply` for a zero-build page. In production you would compile
> and **share** the one state machine so identical code runs everywhere — the
> "determinism across client builds" hazard ADR-0023 calls out. The Node example
> below does exactly that.

## Node example (real shared code)

Runs the **actual** `EdgeReplica` + `BookStateMachine` (no hand-port) and prints a
live table on every change:

```bash
yarn build
node examples/edge-replica/node-example.js http://localhost:3001
```

In another terminal, add books and watch the table update live:

```bash
curl -s -XPOST http://localhost:3001/books -H 'Content-Type: application/json' \
  -d '{"title":"Dune","author":"Herbert","publisher":"Chilton","isbn":"isbn-dune","copies":3}'
```

## Read-your-writes

Reads are eventually consistent by default. After a write commits at index *i*
(the leader returns it), `await replica.waitForIndex(i)` before reading the
affected view so the user sees their own change — see `EdgeReplica.waitForIndex`.
