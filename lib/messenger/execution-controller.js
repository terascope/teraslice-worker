'use strict';

const isNumber = require('lodash/isNumber');
const Server = require('socket.io');
const porty = require('porty');
const Queue = require('@terascope/queue');
const { newId } = require('../utils');
const { closeServer } = require('./helpers');
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
        this.source = 'execution_controller';
        this.workers = {};
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

                this.workers[workerId] = { worker_id: workerId };
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

    onWorkerReady(workerId, timeoutMs) {
        if (this.workers[workerId]) {
            return Promise.resolve({
                worker_id: workerId,
            });
        }
        return this.onceWithTimeout(`worker:ready:${workerId}`, timeoutMs);
    }

    onSliceComplete(workerId, timeoutMs) {
        return this.onceWithTimeout(`worker:slice:complete:${workerId}`, timeoutMs);
    }

    async sendToWorker(workerId, eventName, payload) {
        const message = {
            __source: 'execution_controller',
            payload,
        };
        await this.server.sockets.in(workerId).emit(eventName, message);
    }

    async sendWithResponse(workerId, eventName, payload, timeoutMs) {
        const message = {
            __msgId: newId(),
            __source: this.source,
            response: true,
            payload
        };

        await this.server.sockets.in(workerId).emit(eventName, message);

        let response;
        try {
            response = await this.onceWithTimeout(message.__msgId, timeoutMs);
        } catch (err) {
            throw new Error(`Timeout error while communicating with ${workerId}, with message: ${JSON.stringify(message)}`);
        }

        if (response.error) {
            throw new Error(response.error);
        }

        return response.payload;
    }

    async sendNewSlice(workerId, slice, timeoutMs) {
        await this.onWorkerReady(workerId, timeoutMs);

        const msg = await this.sendWithResponse(workerId, 'slicer:slice:new', slice, timeoutMs);
        if (!msg.willProcess) {
            throw new Error(`Worker ${workerId} will not process new slice`);
        }
        return msg;
    }

    async dispatchSlice(slice, timeoutMs) {
        const requestedWorkerId = slice.request.request_worker;
        const workerId = await this.dequeueWorker(requestedWorkerId);
        return this.sendWithResponse(workerId, 'slicer:slice:new', slice, timeoutMs);
    }

    async dequeueWorker(requestedWorkerId) {
        if (requestedWorkerId) {
            const worker = this.queue.extract('worker_id', requestedWorkerId);
            if (!worker) {
                throw new Error('Invalid slice request');
            }
            return worker.worker_id;
        }
        const worker = this.queue.dequeue();
        if (worker) {
            return worker.worker_id;
        }
        return this.onceWithTimeout('worker:ready');
    }

    async availableWorkers() {
        return this.queue.size();
    }

    _setupDefaultEvents(socket) {
        let workerId;

        socket.on('error', (err) => {
            this._emit('worker:error', {
                worker_id: workerId,
                payload: err
            });
        });

        socket.on('disconnect', (err) => {
            this.flushAny(workerId);
            if (workerId && this.workers[workerId]) {
                delete this.workers[workerId];
            }

            this.queue.remove(workerId, 'worker_id');

            this._emit('worker:disconnect', {
                worker_id: workerId,
                payload: err
            });
        });

        socket.on('worker:ready', (msg, cb) => {
            workerId = msg.worker_id;
            this._enqueueWorker({ worker_id: workerId });

            this.emit('worker:ready', workerId);
            this._emit('worker:ready', msg);

            if (msg.__ack) cb();
        });

        socket.on('messaging:response', (msg) => {
            this.emit(msg.__msgId, msg);
        });
    }

    _onConnection(socket) {
        this._setupDefaultEvents(socket);

        socket.on('worker:slice:complete', (msg, cb) => {
            this._enqueueWorker(msg.payload);
            this._emit('worker:slice:complete', msg);
            if (msg.__ack) cb();
        });
    }

    // do this to make it easier to listen for a specific worker message
    _emit(eventName, { worker_id: workerId, payload }) {
        if (workerId) {
            this.emit(`${eventName}:${workerId}`, payload);
        } else {
            this.emit(`${eventName}`, payload);
        }
    }

    _enqueueWorker({ worker_id: workerId } = {}) {
        if (!workerId) {
            throw new Error('Failed to enqueue invalid worker');
        }

        if (this.queue.exists('worker_id', workerId)) {
            return false;
        }

        this.queue.enqueue({ worker_id: workerId });
        return true;
    }
}

module.exports = ExecutionControllerMessenger;
