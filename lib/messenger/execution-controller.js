'use strict';

const _ = require('lodash');
const Server = require('socket.io');
const porty = require('porty');
const Queue = require('@terascope/queue');
const { getWorkerId } = require('../utils');
const { closeServer } = require('./helpers');
const Messenger = require('./messenger');

class ExecutionControllerMessenger extends Messenger {
    constructor({ port, actionTimeout, networkerLatencyBuffer } = {}) {
        super();
        if (!_.isNumber(port)) {
            throw new Error('ExecutionControllerMessenger requires a valid port');
        }
        this.networkerLatencyBuffer = networkerLatencyBuffer || 0;
        this.actionTimeout = actionTimeout;
        this.port = port;
        this.source = 'execution_controller';
        this.queue = new Queue();
        this.server = new Server();

        this._onConnection = this._onConnection.bind(this);
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is already in-use`);
        }

        this.server.use((socket, next) => {
            const {
                worker_id: workerId
            } = socket.handshake.query;

            socket.join(workerId, (err) => {
                if (err) {
                    next(err);
                    return;
                }

                next();
            });
        });

        this.server.on('connection', this._onConnection);

        this.server.listen(this.port);
    }

    async close() {
        this.queue.each((worker) => {
            this.queue.remove(worker.worker_id, 'worker_id');
        });
        await closeServer(this.server);
        super.close();
    }

    async send(workerId, eventName, payload) {
        const message = {
            message: eventName,
            address: workerId,
            payload,
        };
        return this._send(message);
    }

    async sendWithResponse(workerId, eventName, payload, timeoutMs) {
        const message = {
            message: eventName,
            address: workerId,
            payload
        };
        return this._sendWithResponse(message, timeoutMs);
    }

    async sendNewSlice(workerId, slice, timeoutMs) {
        const msg = await this.sendWithResponse(workerId, 'slicer:slice:new', slice, timeoutMs);

        if (!msg.willProcess) {
            throw new Error(`Worker ${workerId} will not process new slice`);
        }

        return msg;
    }

    async dispatchSlice(slice, timeoutMs) {
        const requestedWorkerId = slice.request.request_worker;
        const workerId = this._workerDequeue(requestedWorkerId);
        if (!workerId) {
            throw new Error('No available workers to dispatch slice to');
        }
        return this.sendWithResponse(workerId, 'slicer:slice:new', slice, timeoutMs);
    }

    availableWorkers() {
        return this.queue.size();
    }

    getClientCounts() {
        return this.server.eio.clientsCount;
    }

    _setupDefaultEvents(socket) {
        let workerId;

        socket.on('error', (err) => {
            this._emit('worker:error', err, [workerId]);
        });

        socket.on('disconnect', (err) => {
            this._workerRemove(workerId);
            this._emit('worker:disconnect', err, [workerId]);
            this.emit('worker:offline');
        });

        socket.on('worker:ready', (msg) => {
            this._workerEnqueue(msg);
        });

        this.handleResponses(socket);
        this.emit('worker:online');
    }

    _onConnection(socket) {
        this._setupDefaultEvents(socket);

        socket.on('worker:slice:complete', (msg) => {
            const workerResponse = msg.payload;
            const sliceId = _.get(workerResponse, 'slice.slice_id');
            const workerId = _.get(workerResponse, 'worker_id');

            if (workerResponse.error) {
                this._emit('slice:failure', workerResponse, [sliceId, workerId]);
            } else {
                this._emit('slice:success', workerResponse, [sliceId, workerId]);
            }

            this.respond(msg, {
                recorded: true,
                slice_id: sliceId,
            });

            this._workerEnqueue(workerResponse);
        });
    }

    _workerEnqueue(arg) {
        const workerId = getWorkerId(arg);
        if (!workerId) {
            throw new Error('Failed to enqueue invalid worker');
        }

        const exists = this.queue.exists('worker_id', workerId);
        if (!exists) {
            this.queue.enqueue({ worker_id: workerId });
        }

        this._emit('worker:enqueue', { worker_id: workerId }, [workerId]);
        return exists;
    }

    _workerDequeue(arg) {
        let workerId;
        if (arg) {
            const worker = this.queue.extract('worker_id', getWorkerId(arg));
            if (worker) workerId = worker.worker_id;
        }

        if (!workerId) {
            const worker = this.queue.dequeue();
            if (worker) workerId = worker.worker_id;
        }

        if (!workerId) {
            return null;
        }

        this._emit('worker:dequeue', { worker_id: workerId }, [workerId]);
        return workerId;
    }

    _workerRemove(arg) {
        const workerId = getWorkerId(arg);
        if (!workerId) return false;

        this.queue.remove(workerId, 'worker_id');

        this._emit('worker:dequeue', { worker_id: workerId }, [workerId]);
        this._emit('worker:remove', { worker_id: workerId }, [workerId]);
        return true;
    }
}

module.exports = ExecutionControllerMessenger;
