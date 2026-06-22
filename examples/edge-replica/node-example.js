/*
 * Node edge read replica (ADR-0023) — the SAME-CODE path.
 *
 * Unlike the browser demo (which hand-ports the reducer for a zero-build page),
 * this runs the REAL EdgeReplica and the REAL BookStateMachine the server runs,
 * so there is a single deterministic state machine shared end to end. It tails a
 * node's committed-log stream and prints the live book list on every change.
 *
 * Run:
 *   yarn build
 *   node examples/edge-replica/node-example.js http://localhost:3001 demo
 *
 * (Start a node first, e.g. `yarn build && node dist/server.js`, or use the
 *  docker-compose cluster and point at http://localhost:3001. The token defaults
 *  to the server's built-in `demo` (all books); pass a scoped token to see only
 *  that slice — ADR-0023 partial replication.)
 */

const { EdgeReplica, HttpStreamSource } = require('../../dist');
const { BookStateMachine } = require('../../dist/models/bookStateMachine');

const url = process.argv[2] || 'http://localhost:3001';
const token = process.argv[3] || 'demo';

const app = new BookStateMachine();
const replica = new EdgeReplica({
    app,
    source: new HttpStreamSource(url, { token }),
    logger: (msg, meta) => console.error(`[edge] ${msg}`, meta ?? ''),
});

function render() {
    const books = app.getAll().sort((a, b) => a.title.localeCompare(b.title));
    console.clear();
    console.log(`Edge replica of ${url}`);
    console.log(`appliedIndex=${replica.lastIndex()} · ${replica.isCaughtUp() ? 'caught up' : 'catching up'} · ${books.length} books\n`);
    if (books.length === 0) {
        console.log('  (no books yet)');
        return;
    }
    for (const b of books) {
        const status = b.borrowedBy ? `borrowed by ${b.borrowedBy}` : `${b.copies}/${b.totalCopies} available`;
        console.log(`  • ${b.title} — ${b.author} [${b.isbn}] (${status})`);
    }
}

replica.onChange(render);
replica.start();
render();

process.on('SIGINT', () => {
    replica.stop();
    process.exit(0);
});
