module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    moduleFileExtensions: ['ts', 'js'],
    transform: {
        '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json', isolatedModules: true }],
    },
    moduleDirectories: ['node_modules', 'src'],
    testMatch: ['**/*.test.ts'],
    // Never discover tests inside git worktrees the tooling parks under .claude/
    // (they contain a full copy of this suite, which would multiply discovery).
    testPathIgnorePatterns: ['/node_modules/', '<rootDir>/.claude/'],
    modulePathIgnorePatterns: ['<rootDir>/.claude/'],
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
    // Floor to stop coverage from regressing, set just below the current aggregate
    // (stmts ~90.4 / branches ~76.7 / funcs ~90.7 / lines ~92.3). A single global
    // bucket is deliberate: jest's per-path/glob thresholds SUBTRACT their files
    // from the global bucket, so adding directory floors would shift the global
    // number unpredictably as code moves — a tightened global floor is the robust
    // regression guard. Sandboxed reducer bodies run inside a vm and are invisible
    // to the instrumenter (their injected counters are no-op'd — see
    // src/runtime/sandbox.ts), which holds the global figures a touch below the
    // bulk of the per-file ones.
    coverageThreshold: {
        global: {
            statements: 88,
            branches: 74,
            functions: 88,
            lines: 90,
        },
    },
};
