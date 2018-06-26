'use strict';

const BaseWorker = require('./base-worker');
const {
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('./teraslice');

class Worker extends BaseWorker {
    constructor(config, jobConfig) {
        super(config, jobConfig);
        this.stores = {};
        this.makeLogger();
    }

    async setup() {
        const { context } = this;
        this.stores.stateStore = await makeStateStore(context);
        this.stores.analyticsStore = await makeAnalyticsStore(context);
    }

    async shutdown() {
        if (this.stores.analyticsStore) {
            await this.stores.analyticsStore.shutdown();
        }
        if (this.stores.stateStore) {
            await this.stores.stateStore.shutdown();
        }
    }
}

module.exports = Worker;
