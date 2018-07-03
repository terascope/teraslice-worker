'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const Server = require('socket.io');
const porty = require('porty');
const {
    sendMessageWithResponse,
    closeServer,
} = require('./helpers');
const MessageEmitter = require('./message-emitter');

class ExecutionControllerMessenger extends MessageEmitter {
    constructor({ port, actionTimeout, networkerLatencyBuffer } = {}) {
        super();
        if (!isNumber(port)) {
            throw new Error('ExecutionControllerMessenger requires a valid port');
        }
        this.networkerLatencyBuffer = networkerLatencyBuffer || 0;
        this.actionTimeout = actionTimeout;
        this.port = port;
        this.workers = {};
        this.server = new Server();
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is already in-use`);
        }

        this.server.on('connection', (socket) => {
            let workerId;

            socket.on('error', (err) => {
                this._emit('worker:error', {
                    worker_id: workerId,
                    payload: err
                });
            });

            socket.on('disconnect', (err) => {
                this.flushAny(workerId);
                if (this.workers[workerId]) {
                    delete this.workers[workerId];
                }
                this._emit('worker:disconnect', {
                    worker_id: workerId,
                    payload: err
                });
            });

            socket.on('worker:ready', (msg) => {
                workerId = msg.worker_id;
                socket.join(workerId);

                this.workers[workerId] = {
                    worker: msg.payload,
                    socket,
                    socketId: socket.id,
                };

                this._emit('worker:ready', msg);
            });

            socket.on('worker:slice:complete', (msg) => {
                this._emit('worker:slice:complete', msg);
            });
        });

        this.server.listen(this.port);
    }

    sendToWorker(workerId, eventName, payload) {
        if (!this.workers[workerId]) {
            throw new Error(`Cannot send message to worker ${workerId}`);
        }
        const { socket } = this.workers[workerId];
        const message = {
            payload,
            __source: 'execution_controller',
        };
        return socket.emit(eventName, message);
    }

    sendNewSlice(workerId, slice) {
        if (!this.workers[workerId]) {
            throw new Error(`Cannot send message to worker ${workerId}`);
        }
        const { socket } = this.workers[workerId];
        return sendMessageWithResponse(socket, 'slicer:slice:new', slice, {
            timeoutMs: this._getTimeout(),
            source: 'execution_controller',
            to: workerId,
        });
    }

    async close() {
        await closeServer(this.server);
        this.flushAll();
        this.removeAllListeners();
    }

    onMessage(eventName, timeoutMs) {
        return this.onceWithTimeout(eventName, this._getTimeout(timeoutMs));
    }

    onWorkerReady(workerId, timeoutMs) {
        if (this.workers[workerId]) {
            return Promise.resolve(this.workers[workerId].worker);
        }
        return this.onMessage(`worker:ready:${workerId}`, timeoutMs);
    }

    onSliceComplete(workerId, timeoutMs) {
        return this.onMessage(`worker:slice:complete:${workerId}`, timeoutMs);
    }

    // do this to make it easier to listen for a specific worker message
    _emit(eventName, { worker_id: workerId, payload }) {
        this.emit(`${eventName}`, {
            worker_id: workerId,
            payload,
        });
        if (workerId) {
            this.emit(`${eventName}:${workerId}`, payload);
        }
    }

    _getTimeout(timeout) {
        return (timeout || this.actionTimeout) + this.networkerLatencyBuffer;
    }
}

module.exports = ExecutionControllerMessenger;
