'use strict';

const porty = require('porty');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    newId,
    overrideLogger,
    terasliceConfig,
    ClusterMasterMessenger,
    newSliceConfig,
} = require('../helpers');

describe('Worker', () => {
    let worker;
    let clusterName;
    let executionController;
    let clusterMaster;
    let es;
    let jobConfig;

    beforeEach(async () => {
        clusterName = `tmp_${shortid.generate()}`.toLowerCase();

        const clusterMasterPort = await porty.find();
        const config = terasliceConfig({ clusterName, clusterMasterPort });
        const slicerPort = await porty.find({ avoids: [clusterMasterPort] });

        clusterMaster = new ClusterMasterMessenger({ port: clusterMasterPort });
        executionController = new ExecutionControllerMessenger({ port: slicerPort });
        await clusterMaster.start();
        await executionController.start();

        jobConfig = {
            type: 'worker',
            job: {
                example: true
            },
            ex_id: newId('ex-id'),
            job_id: newId('job-id'),
            slicer_port: slicerPort,
            slicer_hostname: 'localhost'
        };

        worker = new Worker(config, jobConfig);
        overrideLogger(worker, 'worker');

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

    describe('when the worker is started', () => {
        beforeEach(() => worker.start());

        it('should create the correct stores', () => {
            expect(worker.stores.stateStore).toBeDefined();
            expect(worker.stores.stateStore).toHaveProperty('shutdown');
            expect(worker.stores.analyticsStore).toBeDefined();
            expect(worker.stores.analyticsStore).toHaveProperty('shutdown');
        });

        describe('when a slice is sent from the execution controller', () => {
            let sliceConfig;

            beforeEach(async () => {
                sliceConfig = newSliceConfig();
                await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');
                await executionController.onWorkerReady(worker.workerId);
                await executionController.sendToWorker(worker.workerId, 'slicer:slice:new', sliceConfig);
            });

            it('should return send a slice completed message to the execution controller', async () => {
                const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                expect(msg).toMatchObject({
                    worker_id: worker.workerId,
                    slice: sliceConfig,
                });
                expect(msg).not.toHaveProperty('error');
            });
        });
    });
});
