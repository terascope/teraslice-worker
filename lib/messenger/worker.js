'use strict';

// const Promise = require('bluebird');
const { EventEmitter } = require('events');
const isString = require('lodash/isString');
const io = require('socket.io-client');
const { WrapError } = require('../utils');
const {
    broadcastMessage,
    clientConnect,
    emitMessage
} = require('./helpers');

class WorkerMessenger extends EventEmitter {
    constructor(opts = {}) {
        super();
        const {
            host,
            options,
            workerId
        } = opts;

        if (!isString(host)) {
            throw new Error('WorkerMessenger requires a valid host');
        }

        this.socket = io(host, Object.assign({ forceNew: true, }, options));
        this.workerId = workerId;
    }

    async start() {
        try {
            await clientConnect(this.socket);
        } catch (err) {
            throw new WrapError('Unable to connect to host', err);
        }
    }

    ready() {
        const message = {
            worker_id: this.workerId
        };
        return broadcastMessage(this.socket, 'worker:ready', message);
    }

    send(eventName, message) {
        return emitMessage(this.socket, eventName, message);
    }

    close() {
        return this.socket.close();
    }
}

module.exports = WorkerMessenger;
