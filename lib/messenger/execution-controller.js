'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const isFunction = require('lodash/isFunction');
const Server = require('socket.io');
const porty = require('porty');
const { EventEmitter } = require('events');

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
            throw new Error(`Port ${this.port} is in-use`);
        }

        this.server.on('connection', (socket) => {
            let workerId = 'unknown';

            socket.on('disconnect', (err) => {
                if (this.workers[workerId]) {
                    delete this.workers[workerId];
                }
                this.emit(`${workerId}:disconnect`, err);
            });

            socket.on('worker:ready', (msg) => {
                workerId = msg.worker_id;

                this.server.join(workerId);

                this.workers[workerId] = {
                    worker: msg,
                    socket
                };
                this.emit(`${workerId}:worker:ready`, msg);
            });

            socket.on('error', (err) => {
                this.emit(`${workerId}:error`, err);
            });

            socket.on('worker:slice:complete', (msg, cb) => {
                this.emit(`${workerId}:worker:slice:complete`, msg);
                if (isFunction(cb)) {
                    cb();
                }
            });
        });
        this.server.listen(this.port);
    }

    sendToWorker(workerId, eventName, message) {
        return this.server.sockets.to(workerId).emit(eventName, message);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = ExecutionControllerMessenger;
