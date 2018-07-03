'use strict';

const Promise = require('bluebird');
const { createTempDirSync, cleanupTempDirs } = require('jest-fixtures');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const {
    assetStore: makeAssetStore,
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('../../lib/teraslice');

const exampleReader = require('../fixtures/ops/example-reader');
const exampleOp = require('../fixtures/ops/example-op');

const overrideLogger = require('./override-logger');
const { generateContext } = require('../../lib/utils');
const { newJobConfig, newSysConfig, newSliceConfig } = require('./configs');

class TestContext {
    constructor(testName, options = {}) {
        const {
            clusterMasterPort,
            slicerPort,
            analytics,
            maxRetries
        } = options;

        exampleReader.reader.mockClear();
        exampleReader.newReader.mockClear();

        exampleOp.op.mockClear();
        exampleOp.newProcessor.mockClear();

        this.reader = exampleReader.reader;
        this.newReader = exampleReader.newReader;

        this.op = exampleOp.op;
        this.newProcessor = exampleOp.newProcessor;

        this.clusterName = `tmp_${shortid.generate()}`.toLowerCase();
        this.assetDir = createTempDirSync();

        this.sysconfig = newSysConfig({
            clusterName: this.clusterName,
            assetDir: this.assetDir,
            clusterMasterPort,
        });

        this.jobConfig = newJobConfig({ slicerPort, analytics, maxRetries });

        this.exId = this.jobConfig.ex_id;
        this.jobId = this.jobConfig.job_id;

        this.context = generateContext(this.sysconfig);
        overrideLogger(this.context, testName);

        this.es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });

        this.stores = {};
        this.clean = false;
    }

    async newSlice() {
        this.sliceConfig = newSliceConfig();
        await this.addStateStore();
        await this.stores.stateStore.createState(this.exId, this.sliceConfig, 'start');
    }

    async addAssetStore() {
        if (this.stores.assetStore) return;
        this.stores.assetStore = await makeAssetStore(this.context);
    }

    async addStateStore() {
        if (this.stores.stateStore) return;
        this.stores.stateStore = await makeStateStore(this.context);
    }

    async addAnalyticsStore() {
        if (this.stores.analyticsStore) return;
        this.stores.analyticsStore = await makeAnalyticsStore(this.context);
    }

    async cleanup() {
        if (this.clean) return;

        const stores = Object.values(this.stores);
        await Promise.map(stores, store => store.destroy());

        cleanupTempDirs();

        await this.es.indices.delete({ index: `${this.clusterName}*` });
        this.clean = true;
    }
}

module.exports = TestContext;
