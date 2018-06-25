'use strict';

const { createTempDirSync, cleanupTempDirs } = require('jest-fixtures');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const terasliceConfig = require('./teraslice-config');
const { overrideLoggerOnContext } = require('./override-logger');
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
        overrideLoggerOnContext(this.context, testName);

        this.es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });
    }

    async cleanup() {
        const events = this.context.apis.foundation.getSystemEvents();
        events.removeAllListeners();
        cleanupTempDirs();
        await this.es.indices.delete({ index: `${this.clusterName}*` });
    }
}

module.exports = TestContext;
