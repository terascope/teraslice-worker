'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const isFunction = require('lodash/isFunction');
const Server = require('socket.io');
const porty = require('porty');
const { EventEmitter } = require('events');
const { emitMessage } = require('./helpers');

class ExecutionControllerMessenger extends EventEmitter {
    constructor({ port } = {}) {
        super();
        if (!isNumber(port)) {
            throw new Error('ExecutionControllerMessenger requires a valid port');
        }
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
            let workerId = 'unknown';

            socket.on('error', (err) => {
                this.emit('worker:error', {
                    worker_id: workerId,
                    payload: err,
                });
            });

            socket.on('disconnect', (err) => {
                if (this.workers[workerId]) {
                    delete this.workers[workerId];
                }
                this.emit('worker:disconnect', {
                    worker_id: workerId,
                    payload: err,
                });
            });

            socket.on('worker:ready', (payload, cb) => {
                workerId = payload.worker_id;
                socket.join(workerId);

                this.workers[workerId] = {
                    worker: payload,
                    socket,
                    socketId: socket.id
                };

                this.emit('worker:ready', {
                    worker_id: workerId,
                    payload,
                });

                if (isFunction(cb)) {
                    cb();
                }
            });

            socket.on('worker:slice:complete', (payload, cb) => {
                this.emit('worker:slice:complete', {
                    worker_id: workerId,
                    payload,
                });
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
    }
}

module.exports = ExecutionControllerMessenger;
