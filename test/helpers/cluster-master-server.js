'use strict';

const _ = require('lodash');
const MessengerServer = require('../../lib/messenger/server');

class ClusterMasterServer extends MessengerServer {
    constructor(opts = {}) {
        super(opts);

        this.events = opts.events;
        this.source = 'cluster_master';

        this.clusterAnalytics = {
            slicer: {
                processed: 0,
                failed: 0,
                queued: 0,
                job_duration: 0,
                workers_joined: 0,
                workers_disconnected: 0,
                workers_reconnected: 0
            }
        };

        this._onConnection = this._onConnection.bind(this);
    }

    async start() {
        await this.listen();

        this.server.use((socket, next) => {
            const {
                node_id: nodeId,
            } = socket.handshake.query;

            socket.nodeId = nodeId;
            socket.join(nodeId, (err) => {
                if (err) {
                    next(err);
                    return;
                }

                next();
            });
        });

        this.server.on('connection', this._onConnection);
    }

    async shutdown() {
        await this.close();
    }

    async broadcast(eventName, payload) {
        const message = {
            __source: this.source,
            message: eventName,
            address: '*',
            payload,
        };

        return this._broadcast(message);
    }

    async send(nodeId, eventName, payload) {
        const message = {
            __source: this.source,
            message: eventName,
            address: nodeId,
            node_id: nodeId,
            payload,
        };
        return this._send(message);
    }

    async sendWithResponse(nodeId, eventName, payload, timeoutMs) {
        const message = {
            __source: this.source,
            message: eventName,
            address: nodeId,
            node_id: nodeId,
            payload
        };

        return this._sendWithResponse(message, timeoutMs);
    }

    pauseExecution(nodeId, exId) {
        return this.sendWithResponse(nodeId, 'cluster:execution:pause', { ex_id: exId });
    }

    resumeExecution(nodeId, exId) {
        return this.sendWithResponse(nodeId, 'cluster:execution:resume', { ex_id: exId });
    }

    requestAnalytics(nodeId, exId) {
        return this.sendWithResponse(nodeId, 'cluster:slicer:analytics', { ex_id: exId });
    }

    connectedNodes() {
        return this.server.eio.clientsCount;
    }

    getClusterAnalytics() {
        return _.cloneDeep(this.clusterAnalytics);
    }

    _onConnection(socket) {
        socket.on('error', (err) => {
            this._emit('node:error', err, [socket.nodeId]);
        });

        socket.on('disconnect', (err) => {
            this._emit('node:offline', err, [socket.nodeId]);
        });

        socket.on('cluster:analytics', (msg) => {
            const data = msg.payload;
            if (!this.clusterAnalytics[data.kind]) {
                return;
            }
            _.forOwn(data.stats, (value, field) => {
                if (this.clusterAnalytics[data.kind][field] !== undefined) {
                    this.clusterAnalytics[data.kind][field] += value;
                }
            });
        });

        this.handleResponses(socket);
        this.emit('node:online');
    }
}

module.exports = ClusterMasterServer;
