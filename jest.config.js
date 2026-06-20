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
};
