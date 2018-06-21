'use strict';

const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const { Worker } = require('../../..');
const TerasliceWorker = require('../../../lib');
const overrideLogger = require('../../helpers/override-logger');
const terasliceConfig = require('../../helpers/teraslice-config');

describe('Worker', () => {
    let worker;
    let clusterName;
    let es;

    beforeEach(() => {
        clusterName = `tmp_${shortid.generate()}`.toLowerCase();
        const config = terasliceConfig({ clusterName });
        const jobConfig = {
            type: 'worker',
            job: {
                example: true
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        };
        worker = new Worker(config, jobConfig);
        overrideLogger(worker);
        es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });
    });

    afterEach(async () => {
        await worker.shutdown();
        await es.indices.delete({ index: `${clusterName}*` });
    });

    it('should be an instance of TerasliceWorker', () => {
        expect(worker instanceof TerasliceWorker).toBe(true);
    });

    describe('when setting up', () => {
        beforeEach(() => worker.setup());

        it('should create the correct stores', () => {
            expect(worker.assetStore).toBeDefined();
            expect(worker.assetStore).toHaveProperty('shutdown');
            expect(worker.stateStore).toBeDefined();
            expect(worker.stateStore).toHaveProperty('shutdown');
            expect(worker.analyticsStore).toBeDefined();
            expect(worker.analyticsStore).toHaveProperty('shutdown');
        });
    });
});
