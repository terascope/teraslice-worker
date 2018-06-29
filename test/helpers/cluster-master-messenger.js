'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const isFunction = require('lodash/isFunction');
const Server = require('socket.io');
const porty = require('porty');
const { EventEmitter } = require('events');
const { emitMessage, onMessage } = require('../../lib/messenger/helpers');

class ClusterMasterMessenger extends EventEmitter {
    constructor({ port, timeoutMs = 60000 } = {}) {
        super();
        if (!isNumber(port)) {
            throw new Error('ClusterMaster requires a valid port');
        }
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.workers = {};
        this.server = new Server();
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is already in-use`);
        }

        this.server.on('connection', (socket) => {
            let workerId = 'unknown';

            socket.on('error', (err) => {
                this._emit('worker:error', workerId, err);
            });

            socket.on('disconnect', (err) => {
                if (this.workers[workerId]) {
                    delete this.workers[workerId];
                }
                this._emit('worker:disconnect', workerId, err);
            });

            socket.on('worker:ready', (payload, cb) => {
                workerId = payload.worker_id;
                socket.join(workerId);

                this.workers[workerId] = {
                    worker: payload,
                    socket,
                    socketId: socket.id
                };

                this._emit('worker:ready', workerId, payload);
                if (isFunction(cb)) {
                    cb();
                }
            });

            socket.on('execution:error:terminal', (payload, cb) => {
                this._emit('execution:error:terminal', workerId, payload);
                if (isFunction(cb)) {
                    cb();
                }
            });
        });

        this.server.listen(this.port);
    }

    sendToWorker(workerId, eventName, message) {
        if (!this.workers[workerId]) {
            throw new Error(`Cannot send message to worker ${workerId}`);
        }
        const { socket } = this.workers[workerId];
        return emitMessage(socket, eventName, message);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
        this.removeAllListeners();
    }

    onMessage(eventName, timeoutMs = this.timeoutMs) {
        return onMessage(this, eventName, timeoutMs);
    }

    onWorkerReady(workerId) {
        if (this.workers[workerId]) {
            return Promise.resolve(this.workers[workerId].worker);
        }
        return onMessage(this, `worker:ready:${workerId}`, this.timeoutMs);
    }

    // do this to make it easier to listen for a specific message
    _emit(eventName, workerId, payload) {
        this.emit(`${eventName}`, {
            worker_id: workerId,
            payload,
        });
        this.emit(`${eventName}:${workerId}`, payload);
    }
}

module.exports = ClusterMasterMessenger;
