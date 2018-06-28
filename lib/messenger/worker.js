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
            slicerUrl,
            clusterMasterUrl,
            socketOptions: _socketOptions,
            workerId,
            timeoutMs = 60000
        } = opts;

        if (!isString(slicerUrl)) {
            throw new Error('WorkerMessenger requires a valid slicerUrl');
        }

        if (!isString(clusterMasterUrl)) {
            throw new Error('WorkerMessenger requires a valid clusterMasterUrl');
        }

        if (!isString(workerId)) {
            throw new Error('WorkerMessenger requires a valid workerId');
        }

        this.workerId = workerId;
        this.timeoutMs = timeoutMs;

        const socketOptions = Object.assign({
            forceNew: true,
            autoConnect: false
        }, _socketOptions);

        this.slicerSocket = new SocketIOClient(slicerUrl, socketOptions);
        this.clusterMasterSocket = new SocketIOClient(clusterMasterUrl, socketOptions);
    }

    async start() {
        await this._startSlicer();
        await this._startClusterMaster();
    }

    async broadcast(eventName, message) {
        const slice = this.sendToSlicer(eventName, message);
        const cluster = this.sendToClusterMaster(eventName, message);
        await slice;
        await cluster;
    }

    ready() {
        return this.broadcast('worker:ready', {
            worker_id: this.workerId,
        });
    }

    sendToSlicer(eventName, message) {
        return emitMessage(this.slicerSocket, eventName, message);
    }

    sendToClusterMaster(eventName, message) {
        return emitMessage(this.clusterMasterSocket, eventName, message);
    }

    onMessage(eventName) {
        return onMessage(this, eventName, this.timeoutMs);
    }

    async close() {
        if (this.slicerSocket.connected) {
            this.slicerSocket.close();
        }
        if (this.clusterMasterSocket.connected) {
            this.clusterMasterSocket.close();
        }
    }

    async _startSlicer() {
        try {
            await clientConnect(this.slicerSocket);
        } catch (err) {
            throw new WrapError('Unable to connect to slicer', err);
        }

        this.slicerSocket.on('slicer:slice:new', (msg, cb) => {
            this.emit('slicer:slice:new', msg);
            if (isFunction(cb)) {
                cb();
            }
        });
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
