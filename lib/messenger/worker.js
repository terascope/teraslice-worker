'use strict';

const isString = require('lodash/isString');
const SocketIOClient = require('socket.io-client');
const { WrapError } = require('../utils');
const MessageEmitter = require('./message-emitter');

const { DISABLE_MESSENGER_ACKS } = process.env;

class WorkerMessenger extends MessageEmitter {
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
            autoConnect: false
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
            const responseMsg = {
                __msgId: msg.__msgId,
                __source: 'execution_controller',
                payload: {
                    willProcess: this.available
                }
            };

            this.socket.emit('messaging:response', responseMsg);
            if (this.available) {
                this.emit('slicer:slice:new', msg.payload);
            }
        });

        this.socket.on('slicer:slice:recorded', (msg) => {
            this.emit('slicer:slice:recorded', msg.payload);
        });

        this.socket.on('cluster:error:terminal', (msg) => {
            this.emit('cluster:error:terminal', msg.payload);
        });
    }

    async ready() {
        await this.sendWithAck('worker:ready', { worker_id: this.workerId });
    }

    send(eventName, payload) {
        const message = {
            payload,
            worker_id: this.workerId,
            __source: this.source,
        };

        return this.socket.emit(eventName, message);
    }

    sendWithAck(eventName, payload) {
        if (DISABLE_MESSENGER_ACKS) return this.send(eventName, payload);

        const message = {
            payload,
            worker_id: this.workerId,
            __ack: true,
            __source: this.source,
        };

        return new Promise((resolve) => {
            this.socket.emit(eventName, message, (msg) => { resolve(msg); });
        });
    }

    async sliceComplete({ slice, analyticsData, error }) {
        await this.sendWithAck('worker:slice:complete', {
            worker_id: this.workerId,
            slice,
            analytics: analyticsData,
            error,
        });
    }

    async waitForSlice(fn = () => {}, interval = 100) {
        this.available = true;
        const slice = await new Promise((resolve) => {
            const intervalId = setInterval(() => {
                if (fn()) {
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
            function _cleanup() {
                socket.removeListener('connect', connect);
                socket.removeListener('connect_error', connectError);
                socket.removeListener('connect_timeout', connectError);
            }
            function connect() {
                _cleanup();
                resolve();
            }
            function connectError(err) {
                _cleanup();
                reject(err);
            }

            socket.once('connect', connect);
            socket.once('connect_error', connectError);
            socket.once('connect_timeout', connectError);

            socket.connect();
        });
    }
}

module.exports = WorkerMessenger;
