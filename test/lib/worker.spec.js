'use strict';

const Promise = require('bluebird');
const porty = require('porty');
const shortid = require('shortid');
const ElasticsearchClient = require('elasticsearch').Client;
const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    overrideLogger,
    newSysConfig,
    ClusterMasterMessenger,
    newJobConfig,
    newSliceConfig,
} = require('../helpers');

describe('Worker', () => {
    describe('when constructed', () => {
        let worker;
        let clusterName;
        let executionController;
        let clusterMaster;
        let es;
        let jobConfig;

        beforeEach(async () => {
            clusterName = `tmp_${shortid.generate()}`.toLowerCase();

            const clusterMasterPort = await porty.find();

            clusterMaster = new ClusterMasterMessenger({
                port: clusterMasterPort,
                timeoutMs: 1000
            });

            await clusterMaster.start();

            const slicerPort = await porty.find({ avoids: [clusterMasterPort] });
            executionController = new ExecutionControllerMessenger({
                port: slicerPort,
                timeoutMs: 1000
            });

            await executionController.start();

            const config = newSysConfig({ clusterName, clusterMasterPort });

            jobConfig = newJobConfig({ slicerPort });

            worker = new Worker(jobConfig, config, { timeoutMs: 1000 });
            overrideLogger(worker, 'worker');
            overrideLogger(worker.slice, 'worker:slice');

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

            describe('when a new slice is not sent right away', () => {
                let sliceConfig;

                beforeEach(async () => {
                    sliceConfig = newSliceConfig();
                    await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');
                    await executionController.onWorkerReady(worker.workerId);
                    await Promise.delay(1000);
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

            describe('when a slice errors', () => {
                let sliceConfig;

                beforeEach(async () => {
                    sliceConfig = newSliceConfig();
                    worker.job.queue[1] = jest.fn().mockRejectedValue(new Error('Bad news bears'));
                    await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');
                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendToWorker(worker.workerId, 'slicer:slice:new', sliceConfig);
                });

                it('should return send a slice completed message with an error', async () => {
                    const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: sliceConfig,
                    });
                    expect(msg.error).toStartWith('Error: Slice failed processing, caused by Error: Bad news bears');
                });
            });

            describe('when the slice completes and shutdown is called', () => {
                let sliceConfig;

                beforeEach(async () => {
                    sliceConfig = newSliceConfig();
                    await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');
                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendToWorker(worker.workerId, 'slicer:slice:new', sliceConfig);
                    await executionController.onMessage(`worker:slice:complete:${worker.workerId}`);
                });

                it('should handle the timeout correctly', async () => {
                    expect.hasAssertions();
                    executionController.sendToWorker(worker.workerId, 'slicer:slice:new', sliceConfig);
                    await worker.shutdown();
                    try {
                        await executionController.onMessage(`worker:slice:complete:${worker.workerId}`);
                    } catch (err) {
                        expect(err).not.toBeNil();
                        expect(err.code).toEqual(408);
                    }
                });

                it('should return early if processSlice is called', async () => {
                    expect.hasAssertions();
                    await worker.shutdown();
                    return expect(worker._processSlice(newSliceConfig())).resolves.toBeNil();
                });
            });
        });
    });

    describe('when constructed without nothing', () => {
        it('should throw an error', () => {
            expect(() => {
                new Worker() // eslint-disable-line
            }).toThrow();
        });
    });
});
