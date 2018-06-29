'use strict';

const { EventEmitter } = require('events');
const isString = require('lodash/isString');
const isFunction = require('lodash/isFunction');
const SocketIOClient = require('socket.io-client');
const { WrapError } = require('../utils');
const {
    onMessage,
    emitMessage,
    clientConnect,
} = require('./helpers');

class WorkerMessenger extends EventEmitter {
    constructor(opts = {}) {
        super();
        const {
            executionControllerUrl,
            clusterMasterUrl,
            socketOptions: _socketOptions,
            workerId,
            timeoutMs = 60000
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
        this.timeoutMs = timeoutMs;
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

    sendToExecutionController(eventName, message) {
        return emitMessage(this.slicerSocket, eventName, message);
    }

    sendToClusterMaster(eventName, message) {
        return emitMessage(this.clusterMasterSocket, eventName, message);
    }

    onMessage(eventName, timeoutMs = this.timeoutMs) {
        return onMessage(this, eventName, timeoutMs);
    }

    async close() {
        if (this.slicerSocket.connected) {
            this.slicerSocket.close();
        }
        if (this.clusterMasterSocket.connected) {
            this.clusterMasterSocket.close();
        }
        this.removeAllListeners();
    }

    async _startSlicer() {
        try {
            await clientConnect(this.slicerSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to slicer', err);
        }

        this.slicerSocket.on('slicer:slice:new', (msg, cb) => {
            if (!this.available) {
                cb({ willProcess: false });
                return;
            }
            this.emit('slicer:slice:new', msg);
            this.available = false;
            cb({ willProcess: true });
        });

        this.slicerSocket.on('slicer:slice:recorded', (msg, cb) => {
            this.emit('slicer:slice:recorded', msg);
            this.available = true;
            cb();
        });

        this.available = true;
    }

    async _startClusterMaster() {
        try {
            await clientConnect(this.clusterMasterSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to cluster master', err);
        }

        this.clusterMasterSocket.on('cluster:error:terminal', (msg, cb) => {
            this.emit('cluster:error:terminal', msg);
            if (isFunction(cb)) {
                cb();
            }
        });
    }
}

module.exports = WorkerMessenger;
