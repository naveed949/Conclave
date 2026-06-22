// Browser edge read replica (ADR-0023), zero-build.
//
// This tails a cluster node's committed-log stream (GET /raft/stream) with the
// native EventSource, applies committed book commands to an in-browser store,
// and renders a live-updating list — reads are answered from local memory with
// no network round-trip, and the UI reacts as new commits arrive.
//
// NOTE ON DETERMINISM (ADR-0023 / ADR-0003): the reducer below MUST match the
// server's BookStateMachine.apply exactly, or the replica diverges. Here it is a
// hand-port for a zero-build demo; in production you would compile and SHARE the
// one StateMachine so the same code runs on the server and in every client (the
// "determinism across client builds" hazard the ADR calls out). The Node example
// (node-example.js) does exactly that — it runs the real BookStateMachine.

const $ = (id) => document.getElementById(id);
const params = new URLSearchParams(location.search);
const DEFAULT_NODE = params.get('node') || 'http://localhost:3001';
$('node').value = DEFAULT_NODE;

// ---- the local replica state ----

/** id -> Book. The whole replicated view, served locally. */
let books = new Map();
let appliedIndex = 0;
let caughtUp = false;
let es = null;

/** Faithful port of the server's BookStateMachine.apply (deterministic). */
function applyCommand(cmd) {
    switch (cmd.type) {
        case 'ADD':
            books.set(cmd.book.id, { ...cmd.book });
            break;
        case 'UPDATE': {
            const b = books.get(cmd.id);
            if (b) books.set(cmd.id, { ...b, ...cmd.fields, id: b.id });
            break;
        }
        case 'DELETE':
            books.delete(cmd.id);
            break;
        case 'BORROW': {
            const b = books.get(cmd.id);
            if (b && b.copies > 0) {
                b.copies -= 1;
                b.borrowedBy = cmd.borrowedBy;
                b.borrowedDate = cmd.borrowedDate;
                b.dueDate = cmd.dueDate;
            }
            break;
        }
        case 'RETURN': {
            const b = books.get(cmd.id);
            if (b && b.copies < b.totalCopies) {
                b.copies += 1;
                b.borrowedBy = null;
                b.borrowedDate = null;
                b.dueDate = null;
            }
            break;
        }
        // NOOP / CONFIG: framework control entries, no application effect.
    }
}

// ---- streaming ----

function connect() {
    disconnect();
    const base = $('node').value.replace(/\/$/, '');
    // Reconnect must resume from where we are, so the fromIndex rides the URL.
    es = new EventSource(`${base}/raft/stream?fromIndex=${appliedIndex}`);
    setStatus('connecting…');

    es.addEventListener('snapshot', (e) => {
        const snap = JSON.parse(e.data);
        // The stream carries the replicated-state-machine snapshot; an edge replica
        // only needs the application state slice.
        const state = snap.data && snap.data.state ? snap.data.state : snap.data;
        books = new Map((state || []).map((b) => [b.id, b]));
        appliedIndex = snap.lastIncludedIndex;
        render();
    });

    es.addEventListener('entry', (e) => {
        const { index, entry } = JSON.parse(e.data);
        if (index <= appliedIndex) return; // dedupe replays
        applyCommand(entry.command);
        appliedIndex = index;
        render();
    });

    es.addEventListener('caughtup', (e) => {
        caughtUp = true;
        appliedIndex = JSON.parse(e.data).index;
        render();
    });

    es.onopen = () => setStatus('live');
    es.onerror = () => {
        // EventSource auto-reconnects, but to the ORIGINAL url; close and reopen
        // from the current appliedIndex so the resume is correct.
        setStatus('reconnecting…');
        const wasIndex = appliedIndex;
        disconnect();
        setTimeout(() => {
            appliedIndex = wasIndex;
            connect();
        }, 1500);
    };
}

function disconnect() {
    if (es) {
        es.close();
        es = null;
    }
}

// ---- rendering ----

function setStatus(text) {
    $('status').textContent = text;
    $('status').className = text === 'live' ? 'ok' : 'warn';
}

function render() {
    $('meta').textContent = `appliedIndex=${appliedIndex} · ${caughtUp ? 'caught up' : 'catching up'} · ${books.size} books`;
    const rows = [...books.values()]
        .sort((a, b) => a.title.localeCompare(b.title))
        .map(
            (b) => `<tr>
                <td>${esc(b.title)}</td>
                <td>${esc(b.author)}</td>
                <td>${esc(b.isbn)}</td>
                <td>${b.copies}/${b.totalCopies}</td>
                <td>${b.borrowedBy ? esc(b.borrowedBy) : '—'}</td>
            </tr>`,
        )
        .join('');
    $('books').innerHTML = rows || '<tr><td colspan="5" class="empty">No books yet — add one.</td></tr>';
}

function esc(s) {
    return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// ---- writes go through the leader (NOT the replica) ----

async function addBook(e) {
    e.preventDefault();
    const base = $('node').value.replace(/\/$/, '');
    const body = {
        title: $('title').value,
        author: $('author').value,
        publisher: $('publisher').value || 'Demo Press',
        isbn: $('isbn').value,
        copies: Number($('copies').value) || 1,
    };
    try {
        // The node forwards to the leader if it isn't one; the write then commits
        // and streams back to THIS replica live (watch the row appear).
        const res = await fetch(`${base}/books`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!res.ok) {
            const msg = await res.text();
            alert(`Write failed (${res.status}): ${msg}`);
            return;
        }
        $('addForm').reset();
    } catch (err) {
        alert(`Write error: ${err.message}`);
    }
}

$('addForm').addEventListener('submit', addBook);
$('connect').addEventListener('click', () => {
    appliedIndex = 0;
    caughtUp = false;
    books = new Map();
    connect();
});

connect();
