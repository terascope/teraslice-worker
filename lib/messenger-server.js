'use strict';

const Promise = require('bluebird');
const last = require('lodash/last');
const isFunction = require('lodash/isFunction');
const noop = require('lodash/noop');
const isNumber = require('lodash/isNumber');
const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const forOwn = require('lodash/forOwn');
const Server = require('socket.io');
const porty = require('porty');

class MessengerServer {
    constructor(port, handlers) {
        if (!isNumber(port)) {
            throw new Error('MessengerServer requires a valid port');
        }
        if (isEmpty(handlers) || !isPlainObject(handlers)) {
            throw new Error('MessengerServer requires valid socket handlers');
        }
        this.handlers = handlers;
        this.port = port;
        this.server = new Server();
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is in-use`);
        }

        this.server.on('connection', (socket) => {
            forOwn(this.handlers, (fn, eventName) => {
                socket.on(eventName, async (...args) => {
                    const cb = isFunction(last(args)) ? last(args) : noop;
                    try {
                        const result = await fn();
                        cb(null, result);
                    } catch (err) {
                        cb(err);
                    }
                });
            });
        });

        this.server.listen(this.port);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = MessengerServer;
