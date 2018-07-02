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
    defer,
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
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await clusterMaster.start();

            const slicerPort = await porty.find({ avoids: [clusterMasterPort] });
            executionController = new ExecutionControllerMessenger({
                port: slicerPort,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await executionController.start();

            const config = newSysConfig({ clusterName, clusterMasterPort });

            jobConfig = newJobConfig({ slicerPort });

            worker = new Worker(jobConfig, config);
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
                    await executionController.sendNewSlice(worker.workerId, sliceConfig);
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
                    await executionController.sendNewSlice(worker.workerId, sliceConfig);
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
                    await executionController.sendNewSlice(worker.workerId, sliceConfig);
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
                    await executionController.sendNewSlice(worker.workerId, sliceConfig);
                    await executionController.onMessage(`worker:slice:complete:${worker.workerId}`);
                });

                it('should handle the timeout correctly', async () => {
                    expect.hasAssertions();
                    executionController.sendNewSlice(worker.workerId, sliceConfig);
                    const shutdown = worker.shutdown();
                    try {
                        await executionController.onMessage(`worker:slice:complete:${worker.workerId}`);
                    } catch (err) {
                        expect(err).not.toBeNil();
                        expect(err.code).toEqual(408);
                    }
                    await shutdown;
                });

                it('should return early if processSlice is called', async () => {
                    expect.hasAssertions();
                    await worker.shutdown();
                    return expect(worker._processSlice(newSliceConfig())).resolves.toBeNil();
                });
            });

            describe('when a slice is in-progress and shutdown is called', () => {
                let sliceConfig;
                let workerShutdownEvent;

                beforeEach(async () => {
                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);

                    worker.job.queue[0] = jest.fn(() => Promise.delay(1500));

                    sliceConfig = newSliceConfig();

                    await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');

                    await executionController.onWorkerReady(worker.workerId);
                });

                afterEach(async () => {
                    worker.events.removeListener('worker:shutdown', workerShutdownEvent);
                });

                it('should handle the shutdown properly', async () => {
                    const startTime = Date.now();

                    const sliceMsg = await executionController
                        .sendNewSlice(worker.workerId, sliceConfig);

                    expect(sliceMsg).toEqual({ willProcess: true });

                    const shutdown = worker.shutdown();

                    expect(workerShutdownEvent).toHaveBeenCalled();
                    expect(worker.job.queue[1]).toHaveBeenCalled();

                    const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                    expect(msg).not.toBeNil();

                    await shutdown;
                    const elasped = Date.now() - startTime;
                    expect(elasped).toBeWithin(1500, 3000);
                });
            });

            describe('when a slice is in-progress and has to be forced to shutdown', () => {
                let sliceConfig;
                let workerShutdownEvent;
                let deferred;

                beforeEach(async () => {
                    worker.shutdownTimeout = 1000;

                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);
                    deferred = defer();

                    worker.job.queue[0] = jest.fn(() => deferred.promise);

                    sliceConfig = newSliceConfig();

                    await worker.stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');

                    await executionController.onWorkerReady(worker.workerId);
                });

                afterEach(() => {
                    worker.events.removeListener('worker:shutdown', workerShutdownEvent);
                    deferred.resolve();
                });

                it('should handle the shutdown properly', async () => {
                    expect.assertions(3);

                    const sliceMsg = await executionController
                        .sendNewSlice(worker.workerId, sliceConfig);

                    expect(sliceMsg).toEqual({ willProcess: true });

                    try {
                        await worker.shutdown();
                    } catch (err) {
                        expect(err.message).toEqual('Error: Worker shutdown timeout after 1 seconds, forcing shutdown');
                    }

                    expect(workerShutdownEvent).toHaveBeenCalled();
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
