'use strict';

module.exports = {
    verbose: true,
    testEnvironment: 'node',
    globals: {
        __DEV__: true
    },
    bail: true,
    resetMocks: true,
    setupFiles: ['<rootDir>/test/env-setup.js'],
    collectCoverage: true,
    coverageReporters: ['json', 'lcov', 'text', 'html'],
    coverageDirectory: 'coverage',
    coveragePathIgnorePatterns: [
        '<rootDir>/lib/terafoundation',
        '<rootDir>/lib/teraslice',
        '<rootDir>/test/env-setup.js',
    ],
    coverageThreshold: {
        global: {
            branches: 80,
            functions: 80,
            lines: 80,
            statements: -10
        }
    }
};
