'use strict';

const _ = require('lodash');
const { version } = require('teraslice/package.json');
const { WrapError } = require('../utils');
const MessengerClient = require('../messenger/client');

class ClusterMasterClient extends MessengerClient {
    constructor(opts = {}) {
        const {
            clusterMasterUrl,
            socketOptions: _socketOptions,
            executionContext,
            workerId,
            hostname,
            networkLatencyBuffer,
            actionTimeout,
        } = opts;

        if (!_.isString(clusterMasterUrl)) {
            throw new Error('ClusterMasterClient requires a valid clusterMasterUrl');
        }

        if (_.isEmpty(executionContext)) {
            throw new Error('ClusterMasterClient requires a valid executionContext');
        }

        const {
            job_id: jobId,
            ex_id: exId,
            node_id: nodeId,
        } = executionContext;

        const socketOptions = Object.assign({
            autoConnect: false,
            query: {
                node_id: nodeId,
            }
        }, _socketOptions);

        super({
            hostUrl: clusterMasterUrl,
            socketOptions,
            networkLatencyBuffer,
            actionTimeout
        });

        this.nodeId = nodeId;
        this.source = 'execution_controller';

        this.state = {
            node_id: nodeId,
            hostname,
            pid: process.pid,
            node_version: process.version,
            teraslice_version: version,
            total: 1,
            active: [
                {
                    worker_id: workerId,
                    job_id: jobId,
                    ex_id: exId,
                    assets: [] // TODO
                }
            ],
            available: 0,
            state: 'connected'
        };
    }

    async start() {
        try {
            await this.connect();
        } catch (err) {
            throw new WrapError('Unable to connect to cluster master', err);
        }

        await this.send('node:online', this.state);

        this.socket.on('cluster:slicer:analytics', (msg) => {
            this.emit('cluster:slicer:analytics', msg);
        });

        this.socket.on('cluster:execution:pause', (msg) => {
            this.emit('cluster:execution:pause', msg);
        });

        this.socket.on('cluster:execution:resume', (msg) => {
            this.emit('cluster:execution:resume', msg);
        });

        this.handleResponses(this.socket);
    }

    async shutdown() {
        await this.close();
    }

    async send(eventName, payload) {
        const message = {
            __source: this.source,
            node_id: this.nodeId,
            message: eventName,
            address: this.nodeId,
            payload,
        };

        return this._send(message);
    }

    async sendWithResponse(eventName, payload, timeoutMs) {
        const message = {
            __source: this.source,
            node_id: this.nodeId,
            message: eventName,
            address: this.nodeId,
            payload
        };

        return this._sendWithResponse(message, timeoutMs);
    }

    respond(msg, newMsg) {
        const responseMsg = Object.assign({}, newMsg, {
            __msgId: msg.__msgId,
            __source: msg.__source,
            address: msg.address,
            message: 'messaging:response',
        });

        return this._send(responseMsg);
    }

    updateAnalytics(payload) {
        return this.send('cluster:analytics', payload);
    }
}

module.exports = ClusterMasterClient;
