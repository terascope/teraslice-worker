'use strict';

const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const { Worker } = require('../..');
const BaseWorker = require('../../lib/base-worker');
const { overrideLoggerOnWorker } = require('../helpers/override-logger');
const terasliceConfig = require('../helpers/teraslice-config');

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
            ex_id: 'example-ex-id',
            job_id: 'example-job-id',
            slicer_port: 0
        };

        worker = new Worker(config, jobConfig);
        overrideLoggerOnWorker(worker, 'worker');

        es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });
    });

    afterEach(async () => {
        await worker.shutdown();
        await es.indices.delete({ index: `${clusterName}*` });
    });

    it('should be an instance of BaseWorker', () => {
        expect(worker instanceof BaseWorker).toBe(true);
    });

    describe('when setting up', () => {
        beforeEach(() => worker.setup());

        it('should create the correct stores', () => {
            expect(worker.stores.stateStore).toBeDefined();
            expect(worker.stores.stateStore).toHaveProperty('shutdown');
            expect(worker.stores.analyticsStore).toBeDefined();
            expect(worker.stores.analyticsStore).toHaveProperty('shutdown');
        });
    });
});
