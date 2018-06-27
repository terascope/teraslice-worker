'use strict';

module.exports = {
    verbose: true,
    testEnvironment: 'node',
    globals: {
        __DEV__: true
    },
    bail: false,
    resetMocks: true,
    setupFiles: ['<rootDir>/test/env-setup.js'],
    setupTestFrameworkScriptFile: '<rootDir>/test/jest.setup.js',
    collectCoverage: true,
    coverageReporters: ['json', 'lcov', 'text', 'html'],
    coverageDirectory: 'coverage',
    coveragePathIgnorePatterns: [
        '<rootDir>/lib/teraslice',
        '<rootDir>/lib/terafoundation',
        '<rootDir>/test/helpers',
        '<rootDir>/test/fixtures',
        '<rootDir>/test/env-setup.js',
        '<rootDir>/node_modules'
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
