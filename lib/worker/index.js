'use strict';

const Promise = require('bluebird');
const TerasliceWorker = require('..');
const {
    assetStore: makeAssetStore,
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('../teraslice');

class Worker extends TerasliceWorker {
    constructor(config, jobConfig) {
        super(config, jobConfig);
        this._shutdownSteps = [];
        this.makeLogger();
    }

    setup() {
        const { context } = this;
        return Promise.all([
            makeAssetStore(context),
            makeStateStore(context),
            makeAnalyticsStore(context),
        ]).spread((assetStore, analyticsStore, stateStore) => {
            this.assetStore = assetStore;
            this.analyticsStore = analyticsStore;
            this.stateStore = stateStore;
            this._shutdownSteps.push(() => assetStore.shutdown());
            this._shutdownSteps.push(() => analyticsStore.shutdown());
            this._shutdownSteps.push(() => stateStore.shutdown());
        });
    }

    shutdown() {
        return Promise.map(this._shutdownSteps, fn => fn())
            .then(() => {
                this._shutdownSteps.length = 0;
            });
    }
}

module.exports = Worker;
