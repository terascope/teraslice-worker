'use strict';

const Promise = require('bluebird');

const methods = [
    'bulkSend',
    'count',
    'create',
    'get',
    'index',
    'index_create',
    'index_exists',
    'index_recovery',
    'indexWithId',
    'putTemplate',
    'search',
    'update'
];

const mockElastisearch = {};
methods.forEach((method) => {
    mockElastisearch[method] = jest.fn((...args) => {
        console.warn(`[WARNING]: Implement elasticsearch-api mock for method ${method}. FYI, got args`, ...args);
        return Promise.resolve();
    });
});

module.exports = () => mockElastisearch;
module.exports.mock = mockElastisearch;
