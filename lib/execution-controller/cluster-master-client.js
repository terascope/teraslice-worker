'use strict';

const _ = require('lodash');
const { version } = require('teraslice/package.json');
const WrapError = require('../utils/wrap-error');
const MessengerClient = require('../messenger/client');

class ClusterMasterClient extends MessengerClient {
    constructor(opts = {}) {
        const {
            clusterMasterUrl,
            socketOptions: _socketOptions,
            executionContext,
            workerId,
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
            actionTimeout,
            to: 'cluster_master',
            source: nodeId
        });

        this.nodeId = nodeId;

        this.state = {
            node_id: nodeId,
            hostname: nodeId,
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

        await this.send({
            message: 'node:online',
            node_id: this.nodeId,
            payload: this.state,
        });

        this.handleResponses(this.socket);
    }

    async shutdown() {
        await this.close();
    }

    updateAnalytics(payload) {
        return this.send({
            message: 'cluster:analytics',
            payload
        });
    }
}

module.exports = ClusterMasterClient;
