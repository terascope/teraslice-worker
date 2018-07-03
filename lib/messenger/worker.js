'use strict';

const isString = require('lodash/isString');
const SocketIOClient = require('socket.io-client');
const { WrapError } = require('../utils');
const MessageEmitter = require('./message-emitter');
const { clientConnect } = require('./helpers');

class WorkerMessenger extends MessageEmitter {
    constructor(opts = {}) {
        super();
        const {
            executionControllerUrl,
            clusterMasterUrl,
            socketOptions: _socketOptions,
            workerId,
            networkerLatencyBuffer = 0,
            actionTimeout
        } = opts;

        if (!isString(executionControllerUrl)) {
            throw new Error('WorkerMessenger requires a valid executionControllerUrl');
        }

        if (!isString(clusterMasterUrl)) {
            throw new Error('WorkerMessenger requires a valid clusterMasterUrl');
        }

        if (!isString(workerId)) {
            throw new Error('WorkerMessenger requires a valid workerId');
        }

        this.workerId = workerId;
        this.networkerLatencyBuffer = networkerLatencyBuffer;
        this.actionTimeout = actionTimeout;
        this.available = false;

        const socketOptions = Object.assign({
            forceNew: true,
            autoConnect: false
        }, _socketOptions);

        this.exSocket = new SocketIOClient(executionControllerUrl, socketOptions);
        this.cmSocket = new SocketIOClient(clusterMasterUrl, socketOptions);
    }

    async broadcast(eventName, message) {
        const slice = this.sendToExecutionController(eventName, message);
        const cluster = this.sendToClusterMaster(eventName, message);
        await slice;
        await cluster;
    }

    async close() {
        if (this.exSocket.connected) {
            this.exSocket.close();
        }
        if (this.cmSocket.connected) {
            this.cmSocket.close();
        }
        super.close();
    }

    async start() {
        await this._startExecutionController();
        await this._startClusterMaster();
    }

    async ready() {
        await this.broadcast('worker:ready', {
            worker_id: this.workerId,
        });
    }

    sendToExecutionController(eventName, payload) {
        const message = {
            payload,
            worker_id: this.workerId,
            __source: 'worker'
        };
        return this.exSocket.emit(eventName, message);
    }

    sendToClusterMaster(eventName, payload) {
        const message = {
            payload,
            worker_id: this.workerId,
            __source: 'worker'
        };
        return this.cmSocket.emit(eventName, message);
    }

    sliceComplete({ slice, analyticsData, error }) {
        return this.sendToExecutionController('worker:slice:complete', {
            worker_id: this.workerId,
            slice,
            analytics: analyticsData,
            error,
        });
    }

    async _startExecutionController() {
        try {
            await clientConnect(this.exSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to slicer', err);
        }

        this.exSocket.on('slicer:slice:new', (msg) => {
            const responseMsg = {
                __msgId: msg.__msgId,
                __source: 'execution_controller',
                payload: {
                    willProcess: this.available
                }
            };

            this.exSocket.emit('messaging:response', responseMsg);
            if (this.available) {
                this.emit('slicer:slice:new', msg.payload);
                this.available = false;
            }
        });

        this.exSocket.on('slicer:slice:recorded', (msg) => {
            this.emit('slicer:slice:recorded', msg.payload);
            this.available = true;
        });

        this.available = true;
    }

    async _startClusterMaster() {
        try {
            await clientConnect(this.cmSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to cluster master', err);
        }

        this.cmSocket.on('cluster:error:terminal', (msg) => {
            this.emit('cluster:error:terminal', msg.payload);
        });
    }
}

module.exports = WorkerMessenger;
