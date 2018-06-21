'use strict';

const Promise = require('bluebird');
const TerasliceWorker = require('..');
const { assetStore, stateStore, analyticsStore } = require('../teraslice');

class Worker extends TerasliceWorker {
    constructor(config, jobConfig) {
        super(config, jobConfig);
        this._shutdownSteps = [];
    }

    setup() {
        const { context } = this;
        return Promise.all([
            assetStore(context),
            analyticsStore(context),
            stateStore(context),
        ]).spread((_assetStore, _analyticsStore, _stateStore) => {
            this.assetStore = _assetStore;
            this.analyticsStore = _analyticsStore;
            this.stateStore = _stateStore;
            this._shutdownSteps.push(() => _assetStore.shutdown());
            this._shutdownSteps.push(() => _analyticsStore.shutdown());
            this._shutdownSteps.push(() => _stateStore.shutdown());
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
