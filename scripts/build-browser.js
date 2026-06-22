#!/usr/bin/env node
/*
 * Build the browser edge-replica SDK (ADR-0023) — zero extra dependencies.
 *
 * TypeScript emits ESM but does NOT rewrite relative import specifiers to add the
 * `.js` extension that browsers require for ES modules. So we compile with
 * `tsconfig.browser.json`, then post-process the emitted files to append `.js` to
 * extensionless relative imports. The result under examples/edge-replica/lib/ is
 * loadable directly by a browser `<script type="module">`.
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const libDir = path.join(__dirname, '..', 'examples', 'edge-replica', 'lib');

console.log('tsc -p tsconfig.browser.json …');
execSync('npx tsc -p tsconfig.browser.json', { stdio: 'inherit', cwd: path.join(__dirname, '..') });

/** Append `.js` to relative import/export specifiers that lack an extension. */
function addExtensions(code) {
    const fix = (spec) => (spec.endsWith('.js') ? spec : `${spec}.js`);
    return code
        // import … from './x'  /  export … from '../y/z'
        .replace(/(\bfrom\s*['"])(\.\.?\/[^'"]+?)(['"])/g, (_m, a, spec, c) => a + fix(spec) + c)
        // bare side-effect import './x'
        .replace(/(\bimport\s*['"])(\.\.?\/[^'"]+?)(['"])/g, (_m, a, spec, c) => a + fix(spec) + c);
}

let count = 0;
function walk(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, entry.name);
        if (entry.isDirectory()) walk(p);
        else if (entry.name.endsWith('.js')) {
            fs.writeFileSync(p, addExtensions(fs.readFileSync(p, 'utf8')));
            count += 1;
        }
    }
}
walk(libDir);

console.log(`browser SDK built → examples/edge-replica/lib (${count} modules)`);
