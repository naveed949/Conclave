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

## Browser demo

The page imports the **compiled, shared** SDK — the same `EdgeReplica` and
`BookStateMachine` the server runs — so the browser applies committed commands
with the identical deterministic state machine (no hand-port, no drift). Build it
once, then serve the folder over http (ES modules need an http origin):

```bash
yarn build:browser                                   # emits examples/edge-replica/lib/ (gitignored)
# with a node running on :3001 …
npx --yes http-server examples/edge-replica -p 8080  # or: python3 -m http.server 8080 -d examples/edge-replica
# then visit http://localhost:8080/?node=http://localhost:3001&token=demo
```

Add a book in the form (a write → forwarded to the leader) and watch the row
appear in the list **with no refresh** — it streamed back to the in-page replica.
Open the page in two tabs to see both converge.

The token defaults to the server's built-in `demo` (all books). To see **partial
replication**, run the node with `STREAM_TOKENS="demo=*,acme=Acme Press"` and
connect with token `acme` — the page then receives only that publisher's books.

> `yarn build:browser` compiles `src/edge/browser.ts` to browser ESM via
> `tsconfig.browser.json` and appends `.js` extensions (see `scripts/build-browser.js`),
> with no extra dependency. This is the answer to ADR-0023's "determinism across
> client builds" hazard: ship the one compiled state machine to every client.

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

## Audit verification (M28/M29, browser + Node, full streams)

On an **unfiltered** (no-`StreamGuard`) stream the bootstrap snapshot carries the
audit data, so a replica can re-derive the tamper-evident hash-chain the server
maintains and verify it end to end — **in the browser as well as Node**. The chain
is recomputed with async **WebCrypto** (`globalThis.crypto.subtle`, available in
both browsers and Node 20+), so `verifyAudit()`/`auditHead()` are now async. Opt in
with `verifyAudit: true`:

```js
const replica = new EdgeReplica({
    app: new BookStateMachine(),
    source: new HttpStreamSource('http://localhost:3001'),
    verifyAudit: true, // rebuild + verify the audit chain as we apply
});
replica.start();
await replica.waitForIndex(i); // catch up

const result = await replica.verifyAudit(); // { valid, brokenAt?, length } — or null
console.log(result.valid, await replica.auditHead()); // head equals the node's audit head
```

`await auditHead()` of a caught-up replica equals the node's server-side audit head,
proving the client re-derived the *same* chain (the hash payload format is shared
with the server, so the two cannot drift) — a forged history would fail to verify.
The hasher is injectable via `auditHasher` (default `webcryptoSha256Hex`). One
caveat remains:

- **Scoped streams can't verify.** With a `StreamGuard` the scoped snapshot strips
  the audit (authz vs. a uniform log), so `await verifyAudit()`/`await auditHead()`
  resolve `null` — verification is *unavailable*, never a false "valid". This still
  requires an **unfiltered** stream.
