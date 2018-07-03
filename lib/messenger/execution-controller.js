'use strict';

const Promise = require('bluebird');
const shortid = require('shortid');
const isNumber = require('lodash/isNumber');
const Server = require('socket.io');
const porty = require('porty');
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
        this.server = new Server();

        this._onConnection = this._onConnection.bind(this);
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is already in-use`);
        }

        this.server.on('connection', this._onConnection);

        this.server.listen(this.port);
    }

    async close() {
        await closeServer(this.server);
        super.close();
    }

    onWorkerReady(workerId, timeoutMs) {
        if (this.workers[workerId]) {
            return Promise.resolve(this.workers[workerId].worker);
        }
        return this.onceWithTimeout(`worker:ready:${workerId}`, timeoutMs);
    }

    onSliceComplete(workerId, timeoutMs) {
        return this.onceWithTimeout(`worker:slice:complete:${workerId}`, timeoutMs);
    }

    async sendToWorker(workerId, eventName, payload) {
        if (!this.workers[workerId]) {
            throw new Error(`Cannot send message to worker ${workerId}`);
        }
        const message = {
            __source: 'execution_controller',
            payload,
        };
        await this.server.sockets.in(workerId).emit(eventName, message);
    }

    async sendWithResponse(workerId, eventName, payload, timeoutMs) {
        if (!this.workers[workerId]) {
            throw new Error(`Cannot send message to worker ${workerId}`);
        }

        const message = {
            __msgId: shortid.generate(),
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

    sendNewSlice(workerId, slice, timeoutMs) {
        return this.sendWithResponse(workerId, 'slicer:slice:new', slice, timeoutMs);
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
            socket.join(workerId, () => {
                this.workers[workerId] = {
                    worker: msg.payload,
                    socket,
                    socketId: socket.id,
                };

                this._emit('worker:ready', msg);
            });
        });

        socket.on('messaging:response', (msg) => {
            this.emit(msg.__msgId, msg);
        });
    }

    _onConnection(socket) {
        this._setupDefaultEvents(socket);
        socket.on('worker:slice:complete', (msg) => {
            this._emit('worker:slice:complete', msg);
        });
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
}

module.exports = ExecutionControllerMessenger;
