// Browser edge read replica (ADR-0023) — running the REAL shared code.
//
// This imports the compiled EdgeReplica + EventSourceStreamSource + the SAME
// BookStateMachine the server runs (built by `yarn build:browser` into ./lib/).
// There is no hand-ported reducer here: the browser applies committed commands
// with the identical deterministic state machine, so it cannot drift from the
// server. Reads are answered from local memory; the view updates live as commits
// arrive. The SDK owns reconnection/backoff and resume.
import { EdgeReplica, EventSourceStreamSource, BookStateMachine } from './lib/edge/browser.js';

const $ = (id) => document.getElementById(id);
const params = new URLSearchParams(location.search);
$('node').value = params.get('node') || 'http://localhost:3001';
if (params.get('token')) $('token').value = params.get('token');

let replica = null;
let app = null;

function start() {
    if (replica) replica.stop();
    app = new BookStateMachine();
    const base = $('node').value.replace(/\/$/, '');
    const token = $('token').value.trim();

    // The native EventSource can't set headers, so the token rides the URL (?token=).
    const source = new EventSourceStreamSource(base, EventSource, { token });
    replica = new EdgeReplica({
        app,
        source,
        logger: (msg) => {
            if (msg.includes('error')) setStatus('reconnecting…');
            else if (msg.includes('caught up')) setStatus('live');
        },
    });
    replica.onChange(render);
    setStatus('connecting…');
    replica.start();
    render();
}

// ---- rendering ----

function setStatus(text) {
    $('status').textContent = text;
    $('status').className = text === 'live' ? 'ok' : 'warn';
}

function render() {
    if (!replica || !app) return;
    $('meta').textContent = `appliedIndex=${replica.lastIndex()} · ${replica.isCaughtUp() ? 'caught up' : 'catching up'} · ${app.size()} books`;
    const rows = app
        .getAll()
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
            alert(`Write failed (${res.status}): ${await res.text()}`);
            return;
        }
        $('addForm').reset();
    } catch (err) {
        alert(`Write error: ${err.message}`);
    }
}

$('addForm').addEventListener('submit', addBook);
$('connect').addEventListener('click', start);

start();
