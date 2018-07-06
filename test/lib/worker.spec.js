'use strict';

/* eslint-disable no-console */

const { Worker } = require('../..');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const {
    TestContext,
    findPort,
} = require('../helpers');

describe('Worker', () => {
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

            worker = new Worker(testContext.context, testContext.jobConfig);
        });

        afterEach(async () => {
            await exMessenger.close();
            await worker.shutdown();
            await testContext.cleanup();
        });

        describe('when processing a single slice', () => {
            let sliceConfig;

            beforeEach(async () => {
                await worker.initialize();

                sliceConfig = await testContext.newSlice();
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
                    const workerStart = worker.runOnce();

                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    msg = await exMessenger.onSliceComplete(worker.workerId);

                    await workerStart;
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
                    const workerStart = worker.runOnce();

                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    await Promise.delay(500);

                    msg = await exMessenger.onSliceComplete(worker.workerId, 2000);

                    await workerStart;
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
                it('should return send a slice completed message with an error', async () => {
                    const errMsg = 'Error: Slice failed processing, caused by Error: Bad news bears';
                    const workerStart = worker.runOnce();

                    const newSlice = exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    worker.executionContext.queue[1].mockRejectedValue(new Error('Bad news bears'));

                    try {
                        await workerStart;
                    } catch (err) {
                        expect(err).toStartWith(errMsg);
                    }

                    await newSlice;

                    const msg = await exMessenger.onSliceComplete(worker.workerId);

                    expect(msg).toMatchObject({
                        worker_id: worker.workerId,
                        slice: sliceConfig,
                    });

                    expect(msg.error).toStartWith(errMsg);
                });
            });

            describe('when a slice is in-progress and shutdown is called', () => {
                let workerShutdownEvent;
                let shutdownErr;

                beforeEach(async () => {
                    workerShutdownEvent = jest.fn();
                    worker.events.on('worker:shutdown', workerShutdownEvent);

                    worker.shutdownTimeout = 2000;
                    worker.slice.run = jest.fn(() => Promise.delay(1000));

                    const workerStart = worker.runOnce();

                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    try {
                        await worker.shutdown();
                    } catch (err) {
                        shutdownErr = err;
                    }

                    await workerStart;
                });

                it('should not have a shutdown err', () => {
                    expect(shutdownErr).toBeNil();
                });

                it('should call worker:shutdown', () => {
                    expect(workerShutdownEvent).toHaveBeenCalled();
                });
            });

            describe('when a slice is in-progress and has to be forced to shutdown', () => {
                beforeEach(async () => {
                    worker.shutdownTimeout = 500;

                    worker.slice.run = jest.fn(() => Promise.delay(1000));
                });

                it('shutdown should throw an error', async () => {
                    const workerStart = worker.runOnce();

                    await exMessenger.sendNewSlice(
                        worker.workerId,
                        sliceConfig
                    );

                    const shutdown = worker.shutdown();

                    await Promise.delay(200);

                    expect(worker.slice.run).toHaveBeenCalled();

                    const errMsg = 'Failed to shutdown correctly: Error: Worker shutdown timeout after 0.5 seconds, forcing shutdown';
                    await expect(shutdown).rejects.toThrowError(errMsg);

                    await expect(workerStart).rejects.toThrow();
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
