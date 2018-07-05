'use strict';

const { createTempDirSync, cleanupTempDirs } = require('jest-fixtures');
const shortid = require('shortid');
const path = require('path');
const fs = require('fs-extra');
const ElasticsearchClient = require('elasticsearch').Client;
const {
    makeAssetStore,
    makeStateStore,
    makeAnalyticsStore,
    makeExStore,
} = require('../../lib/teraslice');

const exampleReader = require('../fixtures/ops/example-reader');
const exampleOp = require('../fixtures/ops/example-op');

const overrideLogger = require('./override-logger');
const { generateContext } = require('../../lib/utils');
const { newJobConfig, newSysConfig, newSliceConfig } = require('./configs');
const zipDirectory = require('./zip-directory');

class TestContext {
    constructor(testName, options = {}) {
        const {
            clusterMasterPort,
            slicerPort,
            analytics,
            maxRetries,
            operations,
            assignment,
            assets,
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

        this.jobConfig = newJobConfig({
            assignment,
            slicerPort,
            analytics,
            maxRetries,
            operations,
            assets,
        });

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

    async saveAsset(assetDir, cleanup) {
        await this.addAssetStore();
        const exists = await fs.pathExists(assetDir);
        if (!exists) {
            const err = new Error(`Asset Directory ${assetDir} does not exist`);
            console.error(err.stack); // eslint-disable-line no-console
            throw err;
        }
        const assetZip = await zipDirectory(assetDir);
        const assetId = await this.stores.assetStore.save(assetZip);
        if (cleanup) await fs.remove(path.join(this.assetDir, assetId));
        return assetId;
    }

    async newSlice() {
        this.sliceConfig = newSliceConfig();
        await this.addStateStore();
        await this.stores.stateStore.createState(this.exId, this.sliceConfig, 'start');
    }

    async addAssetStore() {
        if (this.stores.assetStore) return;
        this.stores.assetStore = await makeAssetStore(this.context);
        delete this.context.apis.assets;
    }

    async addStateStore() {
        if (this.stores.stateStore) return;
        this.stores.stateStore = await makeStateStore(this.context);
    }

    async addAnalyticsStore() {
        if (this.stores.analyticsStore) return;
        this.stores.analyticsStore = await makeAnalyticsStore(this.context);
    }

    async addExStore() {
        if (this.stores.exStore) return;
        this.stores.exStore = await makeExStore(this.context);
    }

    async cleanup() {
        if (this.clean) return;

        const stores = Object.values(this.stores);
        await Promise.map(stores, store => store.shutdown(true));

        cleanupTempDirs();

        await this.es.indices.delete({ index: `${this.clusterName}*` });
        this.clean = true;
    }
}

module.exports = TestContext;
