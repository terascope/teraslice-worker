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

        this.slicerSocket = new SocketIOClient(executionControllerUrl, socketOptions);
        this.clusterMasterSocket = new SocketIOClient(clusterMasterUrl, socketOptions);
    }

    async start() {
        await this._startSlicer();
        await this._startClusterMaster();
    }

    async broadcast(eventName, message) {
        const slice = this.sendToExecutionController(eventName, message);
        const cluster = this.sendToClusterMaster(eventName, message);
        await slice;
        await cluster;
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
        return this.slicerSocket.emit(eventName, message);
    }

    sendToClusterMaster(eventName, payload) {
        const message = {
            payload,
            worker_id: this.workerId,
            __source: 'worker'
        };
        return this.clusterMasterSocket.emit(eventName, message);
    }

    onMessage(eventName, timeoutMs) {
        return this.onceWithTimeout(eventName, timeoutMs);
    }

    async close() {
        if (this.slicerSocket.connected) {
            this.slicerSocket.close();
        }
        if (this.clusterMasterSocket.connected) {
            this.clusterMasterSocket.close();
        }
        this.flushAll();
        this.removeAllListeners();
    }

    async _startSlicer() {
        try {
            await clientConnect(this.slicerSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to slicer', err);
        }

        this.slicerSocket.on('slicer:slice:new', (msg) => {
            const responseMsg = {
                __msgId: msg.__msgId,
                __source: 'execution_controller',
                payload: {
                    willProcess: this.available
                }
            };

            this.slicerSocket.emit('messaging:response', responseMsg);
            if (this.available) {
                this.emit('slicer:slice:new', msg.payload);
                this.available = false;
            }
        });

        this.slicerSocket.on('slicer:slice:recorded', (msg) => {
            this.emit('slicer:slice:recorded', msg.payload);
            this.available = true;
        });

        this.available = true;
    }

    async _startClusterMaster() {
        try {
            await clientConnect(this.clusterMasterSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to cluster master', err);
        }

        this.clusterMasterSocket.on('cluster:error:terminal', (msg) => {
            this.emit('cluster:error:terminal', msg.payload);
        });
    }

    sliceComplete({ slice, analyticsData, error }) {
        return this.sendToExecutionController('worker:slice:complete', {
            worker_id: this.workerId,
            slice,
            analytics: analyticsData,
            error,
        });
    }
}

module.exports = WorkerMessenger;
