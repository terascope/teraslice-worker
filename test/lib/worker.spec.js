'use strict';

const porty = require('porty');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const { Worker } = require('../..');
const BaseWorker = require('../../lib/base-worker');
const { overrideLoggerOnWorker } = require('../helpers/override-logger');
const terasliceConfig = require('../helpers/teraslice-config');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const ClusterMasterMessenger = require('../helpers/cluster-master-messenger');

describe('Worker', () => {
    let worker;
    let clusterName;
    let executionController;
    let clusterMaster;
    let es;

    beforeEach(async () => {
        clusterName = `tmp_${shortid.generate()}`.toLowerCase();

        const clusterMasterPort = await porty.find();
        const config = terasliceConfig({ clusterName, clusterMasterPort });
        const slicerPort = await porty.find({ avoids: [clusterMasterPort] });

        clusterMaster = new ClusterMasterMessenger({ port: clusterMasterPort });
        executionController = new ExecutionControllerMessenger({ port: slicerPort });
        await clusterMaster.start();
        await executionController.start();

        const jobConfig = {
            type: 'worker',
            job: {
                example: true
            },
            ex_id: 'example-ex-id',
            job_id: 'example-job-id',
            slicer_port: slicerPort,
            slicer_hostname: 'localhost'
        };

        worker = new Worker(config, jobConfig);
        overrideLoggerOnWorker(worker, 'worker');

        es = new ElasticsearchClient({
            host: 'http://localhost:9200',
            log: '' // This suppresses error logging from the ES library.
        });
    });

    afterEach(async () => {
        await clusterMaster.close();
        await executionController.close();
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
