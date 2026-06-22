module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    moduleFileExtensions: ['ts', 'js'],
    transform: {
        '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json', isolatedModules: true }],
    },
    moduleDirectories: ['node_modules', 'src'],
    testMatch: ['**/*.test.ts'],
    verbose: true,
    // Reporters used when --coverage is passed: a concise summary for CI logs,
    // the per-file table, and lcov (coverage/lcov.info + an HTML report) which CI
    // uploads as an artifact.
    coverageReporters: ['text-summary', 'text', 'lcov'],
    coverageDirectory: 'coverage',
    // What coverage is measured over (only when --coverage is passed). Exclude
    // pure type/wiring entry points that carry no testable logic of their own.
    collectCoverageFrom: [
        'src/**/*.ts',
        '!src/**/types.ts',
        '!src/index.ts',
        '!src/server.ts',
        '!src/moduleServer.ts',
        '!src/edge/browser.ts',
    ],
    // Floor to stop coverage from regressing. Set just below the current numbers
    // so it ratchets up over time rather than blocking on day one. Sandboxed
    // reducer bodies run inside a vm and are not seen by the instrumenter (their
    // injected counters are no-op'd — see src/runtime/sandbox.ts), so global
    // line/branch figures sit a little lower than the bulk of the per-file ones.
    coverageThreshold: {
        global: {
            statements: 86,
            branches: 71,
            functions: 86,
            lines: 88,
        },
    },
};
