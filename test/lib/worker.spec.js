'use strict';

/* eslint-disable no-console */

const Promise = require('bluebird');
const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    TestContext,
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
            let sliceConfig;

            beforeEach(async () => {
                await worker.start();
                await testContext.newSlice();
                ({ sliceConfig } = testContext);
            });

            it('should create the correct stores', () => {
                expect(worker.stores.stateStore).toBeDefined();
                expect(worker.stores.stateStore).toHaveProperty('shutdown');
                expect(worker.stores.analyticsStore).toBeDefined();
                expect(worker.stores.analyticsStore).toHaveProperty('shutdown');
            });

            describe('when a slice is sent from the execution controller', () => {
                let msg;

                beforeEach(async () => {
                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );
                    msg = await executionController.onSliceComplete(worker.workerId);
                });

                it('should return send a slice completed message to the execution controller', () => {
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: sliceConfig,
                    });
                    expect(msg).not.toHaveProperty('error');
                });
            });

            describe('when a new slice is not sent right away', () => {
                let msg;

                beforeEach(async () => {
                    await executionController.onWorkerReady(worker.workerId);
                    await Promise.delay(1000);

                    await executionController.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    msg = await executionController.onSliceComplete(worker.workerId, 2000);
                });

                it('should return send a slice completed message to the execution controller', () => {
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: sliceConfig,
                    });
                    expect(msg).not.toHaveProperty('error');
                });
            });

            describe('when a slice errors', () => {
                let msg;

                beforeEach(async () => {
                    worker.job.queue[1].mockRejectedValue(new Error('Bad news bears'));

                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(worker.workerId, sliceConfig);

                    msg = await executionController.onSliceComplete(worker.workerId);
                });

                it('should return send a slice completed message with an error', async () => {
                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: sliceConfig,
                    });
                    expect(msg.error).toStartWith('Error: Slice failed processing, caused by Error: Bad news bears');
                });
            });

            describe('when a slice is in-progress and shutdown is called', () => {
                let workerShutdownEvent;
                let shutdownErr;

                beforeEach(async () => {
                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);

                    worker.shutdownTimeout = 2000;
                    worker._processSlice = jest.fn(() => Promise.delay(1000));

                    await executionController.onWorkerReady(worker.workerId);
                    await executionController.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    // give the slice time to start being processed
                    await Promise.delay(100);

                    try {
                        await worker.shutdown();
                    } catch (err) {
                        shutdownErr = err;
                    }
                });

                it('should not have a shutdown err', () => {
                    expect(shutdownErr).toBeNil();
                });

                it('should call worker:shutdown', () => {
                    expect(workerShutdownEvent).toHaveBeenCalled();
                });
            });

            describe('when a slice is in-progress and has to be forced to shutdown', () => {
                let shutdown;

                beforeEach(() => executionController.onWorkerReady(worker.workerId));

                beforeEach((done) => {
                    worker.shutdownTimeout = 1000;

                    worker._processSlice = jest.fn(() => {
                        shutdown = worker.shutdown();
                        return Promise.delay(1100);
                    });

                    executionController.sendNewSlice(worker.workerId, sliceConfig).then(() => {
                        let timeout;
                        const interval = setInterval(() => {
                            if (shutdown) {
                                clearInterval(interval);
                                clearTimeout(timeout);
                                done();
                            }
                        }, 10);

                        timeout = setTimeout(() => {
                            clearInterval(interval);
                            expect(worker._processSlice).toHaveBeenCalled();
                            done();
                        }, 2000);
                    }).catch(done.fail);
                });

                it('shutdown should throw an error', () => {
                    const errMsg = 'Failed to shutdown correctly: Error: Worker shutdown timeout after 1 seconds, forcing shutdown';
                    return expect(shutdown).rejects.toThrowError(errMsg);
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
