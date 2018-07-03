'use strict';

/* eslint-disable no-console */

const Promise = require('bluebird');
const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    TestContext,
    newSliceConfig,
    ClusterMasterMessenger,
    findPort,
} = require('../helpers');

describe('Worker', () => {
    describe('when constructed', () => {
        let worker;
        let executionController;
        let clusterMaster;
        let testContext;

        beforeEach(async () => {
            const clusterMasterPort = await findPort();

            clusterMaster = new ClusterMasterMessenger({
                port: clusterMasterPort,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await clusterMaster.start();

            const slicerPort = await findPort();
            executionController = new ExecutionControllerMessenger({
                port: slicerPort,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await executionController.start();

            testContext = new TestContext('worker', { clusterMasterPort, slicerPort });

            worker = new Worker(testContext.context, testContext.jobConfig);
        });

        afterEach(async () => {
            await clusterMaster.close();
            await executionController.close();
            await worker.shutdown();
            await testContext.cleanup();
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
                beforeEach(async () => {
                    await testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );
                });

                it('should return send a slice completed message to the execution controller', async () => {
                    const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: testContext.sliceConfig,
                    });
                    expect(msg).not.toHaveProperty('error');
                });
            });

            describe('when a new slice is not sent right away', () => {
                beforeEach(async () => {
                    await testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);
                    await Promise.delay(1000);

                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );
                });

                it('should return send a slice completed message to the execution controller', async () => {
                    const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: testContext.sliceConfig,
                    });
                    expect(msg).not.toHaveProperty('error');
                });
            });

            describe('when a slice errors', () => {
                beforeEach(async () => {
                    worker.job.queue[1] = jest.fn().mockRejectedValue(new Error('Bad news bears'));

                    await testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );
                });

                it('should return send a slice completed message with an error', async () => {
                    const msg = await executionController.onMessage(`worker:slice:complete:${worker.workerId}`, 2000);
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: testContext.sliceConfig,
                    });
                    expect(msg.error).toStartWith('Error: Slice failed processing, caused by Error: Bad news bears');
                });
            });

            describe('when the slice completes and shutdown is called', () => {
                beforeEach(async () => {
                    await testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);

                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );

                    await executionController.onMessage(`worker:slice:complete:${worker.workerId}`);
                });

                it('should handle the timeout correctly', async () => {
                    expect.hasAssertions();

                    executionController.sendNewSlice(worker.workerId, testContext.sliceConfig);

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
                let workerShutdownEvent;

                beforeEach(async () => {
                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);

                    worker.job.queue[0] = jest.fn(async () => { await Promise.delay(1500); });

                    testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );
                });

                it('should handle the shutdown properly', async () => {
                    const startTime = Date.now();

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
                let workerShutdownEvent;
                let resolveReader;
                let shutdownErr;

                beforeEach(async () => {
                    worker.shutdownTimeout = 1000;

                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);

                    const promise = new Promise((resolve) => {
                        resolveReader = () => {
                            resolve();
                        };
                    });

                    worker.job.queue[0] = jest.fn(() => promise);

                    await testContext.newSlice();

                    await executionController.onWorkerReady(worker.workerId);

                    await executionController.sendNewSlice(
                        worker.workerId,
                        testContext.sliceConfig
                    );

                    await Promise.delay(100);

                    try {
                        await worker.shutdown();
                    } catch (err) {
                        shutdownErr = err;
                    }
                });

                afterEach(() => {
                    resolveReader();
                });

                it('shutdown should throw an error', () => {
                    expect(shutdownErr).not.toBeNil();
                    expect(shutdownErr.message).toEqual('Failed to shutdown correctly: Error: Worker shutdown timeout after 1 seconds, forcing shutdown');
                });

                it('should emit worker:shutdown', () => {
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
