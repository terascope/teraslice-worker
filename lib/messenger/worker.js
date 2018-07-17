'use strict';

const isString = require('lodash/isString');
const pickBy = require('lodash/pickBy');
const SocketIOClient = require('socket.io-client');
const { WrapError } = require('../utils');
const Messenger = require('./messenger');

class WorkerMessenger extends Messenger {
    constructor(opts = {}) {
        super();
        const {
            executionControllerUrl,
            socketOptions: _socketOptions,
            workerId,
            networkerLatencyBuffer = 0,
            actionTimeout
        } = opts;

        if (!isString(executionControllerUrl)) {
            throw new Error('WorkerMessenger requires a valid executionControllerUrl');
        }

        if (!isString(workerId)) {
            throw new Error('WorkerMessenger requires a valid workerId');
        }

        this.workerId = workerId;
        this.networkerLatencyBuffer = networkerLatencyBuffer;
        this.actionTimeout = actionTimeout;
        this.available = false;
        this.source = 'worker';

        const socketOptions = Object.assign({
            forceNew: true,
            autoConnect: false,
            query: {
                worker_id: workerId,
            }
        }, _socketOptions);

        this.socket = new SocketIOClient(executionControllerUrl, socketOptions);
    }

    async close() {
        if (this.socket.connected) {
            this.socket.close();
        }
        super.close();
    }

    async start() {
        try {
            await this._connect();
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
            this.emit('cluster:error:terminal', msg.payload);
        });

        this.handleResponses(this.socket);
    }

    async ready() {
        return this.send('worker:ready', { worker_id: this.workerId });
    }

    async send(eventName, payload) {
        const message = {
            worker_id: this.workerId,
            message: eventName,
            address: this.workerId,
            payload,
        };

        return this._send(message);
    }

    async sendWithResponse(eventName, payload, timeoutMs) {
        const message = {
            worker_id: this.workerId,
            message: eventName,
            address: this.workerId,
            payload
        };
        return this._sendWithResponse(message, timeoutMs);
    }

    sliceComplete({ slice, analytics, error }) {
        return this.sendWithResponse('worker:slice:complete', pickBy({
            worker_id: this.workerId,
            slice,
            analytics,
            error,
        }));
    }

    async waitForSlice(fn = () => {}, interval = 100) {
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

    _connect() {
        const { socket } = this;
        if (socket.connected) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            let connectErr;
            let connectInterval;

            function _cleanup() {
                clearInterval(connectInterval);
                socket.removeListener('connect', connect);
                socket.removeListener('connect_error', connectError);
                socket.removeListener('connect_timeout', connectError);
            }

            function connect() {
                _cleanup();
                resolve();
            }

            function connectError(err) {
                connectErr = err;
            }

            socket.on('connect', connect);
            socket.on('connect_error', connectError);
            socket.on('connect_timeout', connectError);

            socket.connect();

            connectInterval = setInterval(() => {
                _cleanup();
                reject(connectErr);
            }, this.actionTimeout).unref();
        });
    }
}

module.exports = WorkerMessenger;
