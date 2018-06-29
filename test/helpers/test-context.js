'use strict';

const Promise = require('bluebird');
const { createTempDirSync, cleanupTempDirs } = require('jest-fixtures');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const terasliceConfig = require('./teraslice-config');
const {
    assetStore: makeAssetStore,
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('../../lib/teraslice');
const overrideLogger = require('./override-logger');
const { generateContext } = require('../../lib/utils');

class TestContext {
    constructor(testName) {
        this.clusterName = `tmp_${shortid.generate()}`.toLowerCase();
        this.assetDir = createTempDirSync();
        this.config = terasliceConfig({
            clusterName: this.clusterName,
            assetDir: this.assetDir,
        });
        this.context = generateContext(this.config);
        overrideLogger(this.context, testName);

        this.es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });
        this.stores = {};
        this.clean = false;
    }

    async addAssetStore(context) {
        this.stores.assetStore = await makeAssetStore(context);
    }

    async addStateStore(context) {
        this.stores.stateStore = await makeStateStore(context);
    }

    async addAnalyticsStore(context) {
        this.stores.analyticsStore = await makeAnalyticsStore(context);
    }

    async cleanup() {
        if (this.clean) return;

        const stores = Object.values(this.stores);
        await Promise.map(stores, store => store.shutdown());
        const events = this.context.apis.foundation.getSystemEvents();
        events.removeAllListeners();
        cleanupTempDirs();
        await this.es.indices.delete({ index: `${this.clusterName}*` });
        this.clean = true;
    }
}

module.exports = TestContext;
