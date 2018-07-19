'use strict';

const debug = require('debug');

const newLogger = name => ({
    error: jest.fn((...args) => {
        debug(`teraslice-worker:test:${name}:error`)(...args);
    }),
    warn: jest.fn((...args) => {
        debug(`teraslice-worker:test:${name}:warn`)(...args);
    }),
    info: jest.fn((...args) => {
        debug(`teraslice-worker:test:${name}:info`)(...args);
    }),
    trace: jest.fn((...args) => {
        debug(`teraslice-worker:test:${name}:trace`)(...args);
    }),
    debug: jest.fn((...args) => {
        debug(`teraslice-worker:test:${name}:debug`)(...args);
    }),
    streams: [],
    flush: jest.fn(() => Promise.resolve())
});

function overrideLoggerOnContext(context, name = 'idk') {
    context.logger = newLogger(`${name}:terafoundation`);
    context.apis.foundation.makeLogger = ({ module }) => newLogger(`${name}:${module}`);
    return context;
}

function overrideLoggerOnClass(worker, name = 'idk') {
    worker.logger = newLogger(`${name}`);
    worker.context.logger = newLogger(`${name}:terafoundation`);
    worker.context.apis.foundation.makeLogger = ({ module }) => newLogger(`${name}:${module}`);
    return worker;
}

module.exports = (input, name) => {
    if (input.context) {
        return overrideLoggerOnClass(input, name);
    }
    return overrideLoggerOnContext(input, name);
};
