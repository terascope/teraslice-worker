'use strict';

const isString = require('lodash/isString');
const { WrapError } = require('../utils');
const MessengerClient = require('../messenger/client');

class ClusterMasterClient extends MessengerClient {
    constructor(opts = {}) {
        const {
            clusterMasterUrl,
            socketOptions: _socketOptions,
            nodeId,
            networkerLatencyBuffer,
            actionTimeout
        } = opts;

        if (!isString(clusterMasterUrl)) {
            throw new Error('ClusterMasterMessenger requires a valid clusterMasterUrl');
        }

        if (!isString(nodeId)) {
            throw new Error('ClusterMasterMessenger requires a valid nodeId');
        }

        const socketOptions = Object.assign({
            autoConnect: false,
            query: {
                node_id: nodeId,
            }
        }, _socketOptions);

        super({
            hostUrl: clusterMasterUrl,
            socketOptions,
            networkerLatencyBuffer,
            actionTimeout
        });

        this.nodeId = nodeId;
        this.source = 'execution_controller';
    }

    async start() {
        try {
            await this.connect();
        } catch (err) {
            throw new WrapError('Unable to connect to cluster master', err);
        }

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

    updateAnalytics(payload) {
        return this.send('cluster:analytics', payload);
    }

    executionTerminal(exId) {
        return this.send('execution:error:terminal', { ex_id: exId });
    }

    executionFinished(exId) {
        return this.send('execution:finished', { ex_id: exId });
    }
}

module.exports = ClusterMasterClient;
