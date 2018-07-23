'use strict';

const times = require('lodash/times');
const random = require('lodash/random');
const { EventEmitter } = require('events');
const { ExecutionController } = require('../../..');
const WorkerMessenger = require('../../../lib/worker/messenger');
const { TestContext, findPort, newId } = require('../../helpers');

describe('ExecutionController', () => {
    // [ message, config ]
    const testCases = [
        [
            'one slice',
            {
                slicerResults: [
                    { example: 'single-slice' },
                    null
                ],
                body: { example: 'single-slice' },
                count: 1,
                workers: 1,
                lifecycle: 'once',
                analytics: false
            }
        ],
        [
            'sub-slices',
            {
                slicerResults: [
                    [
                        { example: 'subslice' },
                        { example: 'subslice' },
                        { example: 'subslice' },
                    ],
                    null,
                ],
                count: 3,
                workers: 1,
                body: { example: 'subslice' },
                lifecycle: 'once',
                analytics: true,
            }
        ],
        [
            'a slice and the second slice throws an error',
            {
                slicerResults: [
                    { example: 'slice-failure' },
                    new Error('Slice failure'),
                    null
                ],
                body: { example: 'slice-failure' },
                count: 1,
                workers: 1,
                lifecycle: 'once',
                analytics: true,
            }
        ],
        [
            'slices with multiple workers and one reconnects',
            {
                slicerResults: [
                    { example: 'slice-disconnect' },
                    { example: 'slice-disconnect' },
                    { example: 'slice-disconnect' },
                    { example: 'slice-disconnect' },
                    null
                ],
                reconnect: true,
                body: { example: 'slice-disconnect' },
                count: 4,
                workers: 1,
                lifecycle: 'once',
                analytics: true,
            }
        ],
        [
            'a slice with dynamic queue length',
            {
                slicerResults: [
                    { example: 'slice-dynamic' },
                    { example: 'slice-dynamic' },
                    { example: 'slice-dynamic' },
                    { example: 'slice-dynamic' },
                    null
                ],
                reconnect: true,
                slicerQueueLength: 'QUEUE_MINIMUM_SIZE',
                body: { example: 'slice-dynamic' },
                count: 4,
                workers: 2,
                lifecycle: 'once',
                analytics: false,
            }
        ],
        [
            'a slice that fails',
            {
                slicerResults: [
                    { example: 'slice-fail' },
                    null
                ],
                sliceFailed: true,
                body: { example: 'slice-fail' },
                count: 1,
                workers: 1,
                lifecycle: 'once',
                analytics: false,
            }
        ],
    ];

    describe.each(testCases)('when processing %s', (m, options) => {
        // give this test extra time
        jest.setTimeout(15 * 1000);

        const {
            slicerResults,
            slicerQueueLength,
            count,
            lifecycle,
            body,
            reconnect,
            analytics,
            workers,
            sliceFailed,
        } = options;

        let exController;
        let testContext;
        let slices;
        let exStore;
        let stateStore;

        beforeEach(async () => {
            slices = [];

            await TestContext.cleanupAll();

            const port = await findPort();

            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
                slicerPort: port,
                slicerQueueLength,
                slicerResults,
                lifecycle,
                workers,
                analytics,
            });

            await testContext.makeItARealJob();

            exController = new ExecutionController(testContext.context, testContext.config);
            const {
                network_latency_buffer: networkerLatencyBuffer,
                action_timeout: actionTimeout,
            } = testContext.context.sysconfig.teraslice;

            testContext.attachCleanup(() => exController.shutdown());

            await testContext.addStateStore();
            await testContext.addExStore();
            ({ stateStore, exStore } = testContext.stores);

            const opCount = testContext.config.job.operations.length;

            await exController.initialize();
            const doneProcessing = () => slices.length >= count;

            const socketOptions = reconnect ? {
                timeout: 1000,
                reconnection: true,
                reconnectionAttempts: 10,
                reconnectionDelay: 500,
                reconnectionDelayMax: 500
            } : {
                timeout: 1000,
                reconnection: false
            };

            let firedReconnect = false;

            async function startWorker() {
                const workerMessenger = new WorkerMessenger({
                    executionControllerUrl: `http://localhost:${port}`,
                    workerId: newId('worker'),
                    networkerLatencyBuffer,
                    actionTimeout,
                    events: new EventEmitter(),
                    socketOptions
                });

                testContext.attachCleanup(() => workerMessenger.shutdown());

                await workerMessenger.start();

                async function waitForReconnect() {
                    if (!reconnect) return;
                    if (firedReconnect) return;

                    firedReconnect = true;
                    await Promise.all([
                        workerMessenger.forceReconnect(),
                        exController.messenger.onceWithTimeout('worker:reconnect', 5 * 1000)
                    ]);
                }

                async function process() {
                    if (doneProcessing()) return;

                    const slice = await workerMessenger.waitForSlice(doneProcessing);

                    if (!slice) return;

                    slices.push(slice);

                    const msg = { slice };

                    if (analytics) {
                        msg.analytics = {
                            time: times(opCount, () => random(0, 2000)),
                            size: times(opCount, () => random(0, 100)),
                            memory: times(opCount, () => random(0, 10000)),
                        };
                    }

                    if (sliceFailed) {
                        msg.error = 'Oh no, slice failure';
                        await stateStore.updateState(slice, 'error', msg.error);
                    } else {
                        await stateStore.updateState(slice, 'completed');
                    }

                    await Promise.all([
                        waitForReconnect(),
                        Promise.delay().then(() => workerMessenger.sliceComplete(msg)),
                    ]);

                    await process();
                }

                await process();

                await workerMessenger.shutdown();
            }

            function startWorkers() {
                return Promise.all(times(workers, startWorker));
            }

            await Promise.all([
                startWorkers(),
                exController.run(),
            ]);
        });

        afterEach(() => testContext.cleanup());

        it('should process the execution correctly correctly', async () => {
            const { exId } = testContext;

            expect(slices).toBeArrayOfSize(count);
            times(count, (i) => {
                const slice = slices[i];
                expect(slice).toHaveProperty('request');
                expect(slice.request).toEqual(body);
            });

            const exStatus = await exStore.get(exId);
            expect(exStatus).toBeObject();
            expect(exStatus).toHaveProperty('_slicer_stats');

            if (sliceFailed) {
                expect(exStatus).toHaveProperty('_failureReason', `execution: ${exId} had 1 slice failures during processing`);
                expect(exStatus._slicer_stats.failed).toBeGreaterThan(0);
                expect(exStatus).toHaveProperty('_has_errors', true);
                expect(exStatus).toHaveProperty('_status', 'failed');

                const query = `ex_id:${exId} AND state:error`;
                const actualCount = await stateStore.count(query, 0);
                expect(actualCount).toEqual(count);
            } else {
                expect(exStatus).toHaveProperty('_status', 'completed');
                expect(exStatus).toHaveProperty('_has_errors', false);
                expect(exStatus._slicer_stats.processed).toBeGreaterThan(0);

                const query = `ex_id:${exId} AND state:completed`;
                const actualCount = await stateStore.count(query, 0);
                expect(actualCount).toEqual(count);
            }

            if (reconnect && slicerQueueLength !== 'QUEUE_MINIMUM_SIZE') {
                expect(exStatus._slicer_stats.workers_reconnected).toBeGreaterThan(0);
            }
        });
    });

    describe('when testing shutdown', () => {
        let testContext;
        let exController;

        beforeEach(() => {
            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
            });
            exController = new ExecutionController(testContext.context, testContext.config);
        });

        afterEach(() => testContext.cleanup());

        describe('when not initialized', () => {
            it('should resolve', () => expect(exController.shutdown()).resolves.toBeNil());
        });

        describe('when initialized', () => {
            beforeEach(() => {
                exController.isInitialized = true;
            });

            describe('when controller is already being shutdown', () => {
                beforeEach(() => {
                    exController.isShuttingDown = true;
                });

                it('should resolve', () => expect(exController.shutdown()).resolves.toBeNil());
            });

            describe('when everything errors', () => {
                beforeEach(() => {
                    exController.isDone = () => false;
                    exController._doneProcessing = () => Promise.reject(new Error('Slicer Finish Error'));

                    exController.stores = {};
                    exController.stores.someStore = {
                        shutdown: () => Promise.reject(new Error('Store Error'))
                    };

                    exController.engine = {};
                    exController.engine.shutdown = () => Promise.reject(new Error('Engine Error'));

                    exController.executionAnalytics = {};
                    exController.executionAnalytics.shutdown = () => Promise.reject(new Error('Execution Analytics Error'));

                    exController.job = {};
                    exController.job.shutdown = () => Promise.reject(new Error('Job Error'));

                    exController.messenger = {};
                    exController.messenger.shutdown = () => Promise.reject(new Error('Messenger Error'));
                });

                it('should reject with all of the errors', async () => {
                    expect.hasAssertions();
                    try {
                        await exController.shutdown();
                    } catch (err) {
                        const errMsg = err.toString();
                        expect(errMsg).toStartWith('Error: Failed to shutdown correctly');
                        expect(errMsg).toInclude('Slicer Finish Error');
                        expect(errMsg).toInclude('Store Error');
                        expect(errMsg).toInclude('Engine Error');
                        expect(errMsg).toInclude('Execution Analytics Error');
                        expect(errMsg).toInclude('Job Error');
                        expect(errMsg).toInclude('Messenger Error');
                    }
                });
            });
        });
    });
});
