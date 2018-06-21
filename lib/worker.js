'use strict';

const TerasliceWorker = require('.');
const { assetStore, stateStore, analyticsStore } = require('./teraslice');

class Worker extends TerasliceWorker {
    async start() {
        this.assetStore = await assetStore(this.context);
        this.stateStore = await stateStore(this.context);
        this.analyticsStore = await analyticsStore(this.context);
        this.started = true;
    }

    async shutdown() {
        if (!this.started) return;

        await this.assetStore.shutdown();
        await this.stateStore.shutdown();
        await this.analyticsStore.shutdown();
        this.started = false;
    }
}

module.exports = Worker;
