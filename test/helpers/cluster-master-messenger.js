'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const toNumber = require('lodash/toNumber');
const Server = require('socket.io');
const porty = require('porty');
const { EventEmitter } = require('events');
const { onMessage } = require('../../lib/messenger/helpers');

class ClusterMasterMessenger extends EventEmitter {
    constructor({ port, actionTimeout, networkerLatencyBuffer } = {}) {
        super();
        if (!isNumber(port)) {
            throw new Error('ClusterMaster requires a valid port');
        }
        this.port = port;
        this.networkerLatencyBuffer = networkerLatencyBuffer || 0;
        this.actionTimeout = actionTimeout;
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
                    socketId: socket.id
                };

                this._emit('worker:ready', msg);
            });

            socket.on('execution:error:terminal', (msg) => {
                this._emit('execution:error:terminal', msg);
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

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
        this.removeAllListeners();
    }

    onMessage(eventName, timeoutMs) {
        return onMessage(this, eventName, this._getTimeout(timeoutMs));
    }

    onWorkerReady(workerId) {
        if (this.workers[workerId]) {
            return Promise.resolve(this.workers[workerId].worker);
        }
        return onMessage(this, `worker:ready:${workerId}`, this._getTimeout());
    }

    // do this to make it easier to listen for a specific worker message
    _emit(eventName, { worker_id: workerId, payload }) {
        this.emit(`${eventName}`, {
            worker_id: workerId,
            payload,
        });
        this.emit(`${eventName}:${workerId}`, payload);
    }

    _getTimeout(timeout = this.actionTimeout) {
        return toNumber(timeout) + this.networkerLatencyBuffer;
    }
}

module.exports = ClusterMasterMessenger;
