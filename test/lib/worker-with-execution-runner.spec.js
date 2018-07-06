'use strict';

/* eslint-disable no-console */

const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    TestContext,
    findPort,
} = require('../helpers');

xdescribe('Worker (with execution runner)', () => {
    describe('when constructed', () => {
        let worker;
        let exMessenger;
        let testContext;

        beforeEach(async () => {
            const slicerPort = await findPort();
            exMessenger = new ExecutionControllerMessenger({
                port: slicerPort,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await exMessenger.start();

            testContext = new TestContext('worker', { slicerPort });

            worker = new Worker(testContext.context, testContext.jobConfig, true);
        });

        afterEach(async () => {
            await exMessenger.close();
            await worker.shutdown();
            await testContext.cleanup();
        });

        describe('when the worker is started', () => {
            let sliceConfig;
            let workerStart;

            beforeEach(async () => {
                await worker.initialize();
                workerStart = worker.start();
                await testContext.newSlice();
                ({ sliceConfig } = testContext);
            });

            afterEach(async () => {
                await workerStart;
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
                    await exMessenger.onWorkerReady(worker.workerId);
                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );
                    msg = await exMessenger.onSliceComplete(worker.workerId);
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
                    await exMessenger.onWorkerReady(worker.workerId);
                    await Promise.delay(1000);

                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    msg = await exMessenger.onSliceComplete(worker.workerId, 2000);
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
                    worker.executionContext.queue[1].mockRejectedValue(new Error('Bad news bears'));

                    await exMessenger.onWorkerReady(worker.workerId);
                    await exMessenger.sendNewSlice(worker.workerId, sliceConfig);

                    msg = await exMessenger.onSliceComplete(worker.workerId);
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

                    await exMessenger.onWorkerReady(worker.workerId);
                    await exMessenger.sendNewSlice(
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

                beforeEach(() => exMessenger.onWorkerReady(worker.workerId));

                beforeEach((done) => {
                    worker.shutdownTimeout = 1000;

                    worker._processSlice = jest.fn(() => {
                        shutdown = worker.shutdown();
                        return Promise.delay(1100);
                    });

                    exMessenger.sendNewSlice(worker.workerId, sliceConfig).then(() => {
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
