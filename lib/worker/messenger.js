'use strict';

const isString = require('lodash/isString');
const pickBy = require('lodash/pickBy');
const { WrapError } = require('../utils');
const MessengerClient = require('../messenger/client');

class WorkerMessenger extends MessengerClient {
    constructor(opts = {}) {
        const {
            executionControllerUrl,
            socketOptions: _socketOptions,
            workerId,
            networkerLatencyBuffer,
            actionTimeout,
            events
        } = opts;

        if (!isString(executionControllerUrl)) {
            throw new Error('WorkerMessenger requires a valid executionControllerUrl');
        }

        if (!isString(workerId)) {
            throw new Error('WorkerMessenger requires a valid workerId');
        }

        const socketOptions = Object.assign({
            autoConnect: false,
            query: {
                worker_id: workerId,
            }
        }, _socketOptions);

        super({
            hostUrl: executionControllerUrl,
            socketOptions,
            networkerLatencyBuffer,
            actionTimeout
        });

        this.events = events;
        this.workerId = workerId;
        this.source = 'worker';
        this.available = false;
    }

    async start() {
        try {
            await this.connect();
        } catch (err) {
            throw new WrapError('Unable to connect to execution controller', err);
        }

        this.socket.on('slicer:slice:new', (msg) => {
            this.respond(msg, { willProcess: this.available });
            if (this.available) {
                this.emit('slicer:slice:new', msg.payload);
            }
        });

        this.socket.on('cluster:error:terminal', (msg) => {
            this.events.emit('worker:shutdown', msg.payload);
        });

        this.socket.on('execution:finished', (msg) => {
            this.events.emit('worker:shutdown', msg.payload);
        });

        this.socket.on('execution:error:terminal', (msg) => {
            this.events.emit('worker:shutdown', msg.payload);
        });

        this.handleResponses(this.socket);
    }

    async shutdown() {
        await this.close();
    }

    async ready() {
        return this.send('worker:ready', { worker_id: this.workerId });
    }

    async send(eventName, payload) {
        const message = {
            __source: this.source,
            worker_id: this.workerId,
            message: eventName,
            address: this.workerId,
            payload,
        };

        return this._send(message);
    }

    async sendWithResponse(eventName, payload, timeoutMs, retry) {
        const message = {
            __source: this.source,
            worker_id: this.workerId,
            message: eventName,
            address: this.workerId,
            payload
        };
        return this._sendWithResponse(message, timeoutMs, retry);
    }

    sliceComplete(msg) {
        const message = Object.assign({ worker_id: this.workerId }, msg);
        return this.sendWithResponse('worker:slice:complete', pickBy(message), null, true);
    }

    async waitForSlice(fn = () => {}, interval = 100) {
        this.ready();
        this.available = true;

        const slice = await new Promise((resolve) => {
            const intervalId = setInterval(() => {
                if (this.closed || fn()) {
                    this.removeListener('slicer:slice:new', onMessage);
                    resolve();
                }
            }, interval);
            function onMessage(msg) {
                clearInterval(intervalId);
                resolve(msg);
                this.available = false;
            }
            this.once('slicer:slice:new', onMessage);
        });

        this.available = false;

        return slice;
    }
}

module.exports = WorkerMessenger;