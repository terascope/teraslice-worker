'use strict';

const isNumber = require('lodash/isNumber');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');

class ClusterMasterMessenger extends ExecutionControllerMessenger {
    constructor(options = {}) {
        if (!isNumber(options.port)) {
            throw new Error('ClusterMaster requires a valid port');
        }
        super(options);
        this.source = 'cluster_master';
        this._onConnection = this._onConnection.bind(this);
    }

    _onConnection(socket) {
        this._setupDefaultEvents(socket);

        socket.on('execution:error:terminal', (msg) => {
            this._emit('execution:error:terminal', msg);
        });
    }
}

module.exports = ClusterMasterMessenger;
