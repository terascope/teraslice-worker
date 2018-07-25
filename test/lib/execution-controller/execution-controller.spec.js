'use strict';

const _ = require('lodash');
const { EventEmitter } = require('events');
const { ExecutionController } = require('../../..');
const WorkerMessenger = require('../../../lib/worker/messenger');
const { TestContext, findPort, newId } = require('../../helpers');


describe('ExecutionController', () => {
    describe.each([
        ['the new implementation', false],
        ['existing execution runner', true]
    ])('when using the %s', (_ignore, useExecutionRunner) => {
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
                }
            ],
            [
                'a slicer requests a specific worker',
                {
                    slicerResults: [
                        { request_worker: 'specific-worker-1', example: 'specific-worker' },
                        null
                    ],
                    workerIds: ['specific-worker-1'],
                    body: { request_worker: 'specific-worker-1', example: 'specific-worker' },
                    count: 1,
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
                    emitSlicerRecursion: true,
                    count: 3,
                    body: { example: 'subslice' },
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
                }
            ],
            [
                'a slicer that emits a "slicer:execution:update" event',
                {
                    slicerResults: [
                        { example: 'slice-execution-update' },
                        null
                    ],
                    emitsExecutionUpdate: [
                        {
                            _op: 'some-example',
                            newData: true
                        }
                    ],
                    body: { example: 'slice-execution-update' },
                    count: 1,
                }
            ],
            [
                'a slicer that emits a "slicer:slice:range_expansion" event',
                {
                    slicerResults: [
                        { example: 'slicer-slice-range-expansion' },
                        null
                    ],
                    emitSlicerRangeExpansion: true,
                    body: { example: 'slicer-slice-range-expansion' },
                    count: 1,
                }
            ],
            [
                'a slice and the execution is paused and resumed',
                {
                    slicerResults: [
                        { example: 'slice-pause-and-resume' },
                        null
                    ],
                    pauseAndResume: true,
                    body: { example: 'slice-pause-and-resume' },
                    count: 1,
                }
            ],
        ];

        // fdescribe.each([testCases[testCases.length - 1]])('when processing %s', (m, options) => {
        describe.each(testCases)('when processing %s', (m, options) => {
        // give this test extra time
            jest.setTimeout(15 * 1000);

            const {
                slicerResults,
                slicerQueueLength,
                count,
                lifecycle = 'once',
                body,
                reconnect = false,
                analytics = false,
                workers = 1,
                pauseAndResume = false,
                sliceFailed = false,
                emitsExecutionUpdate,
                emitSlicerRecursion = false,
                emitSlicerRangeExpansion = false,
                workerIds = [],
            } = options;

            let exController;
            let testContext;
            let slices;
            let exStore;
            let stateStore;
            let defaultClusterAnalytics;

            beforeEach(async () => {
                slices = [];

                await TestContext.cleanupAll();

                const port = await findPort();

                testContext = new TestContext({
                    assignment: 'execution_controller',
                    slicerPort: port,
                    slicerQueueLength,
                    slicerResults,
                    lifecycle,
                    workers,
                    analytics,
                    useExecutionRunner,
                });

                await testContext.addClusterMaster();

                await testContext.initialize(true);

                const { clusterMaster, exId, nodeId } = testContext;
                defaultClusterAnalytics = clusterMaster.getClusterAnalytics();

                exController = new ExecutionController(
                    testContext.context,
                    testContext.executionContext,
                );

                const {
                    network_latency_buffer: networkerLatencyBuffer,
                    action_timeout: actionTimeout,
                } = testContext.context.sysconfig.teraslice;

                testContext.attachCleanup(() => exController.shutdown());

                await testContext.addStateStore();
                await testContext.addExStore();
                ({ stateStore, exStore } = testContext.stores);

                const opCount = testContext.executionContext.config.operations.length;

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


                async function startWorker(n) {
                    const workerId = workerIds[n] || newId('worker');
                    const workerMessenger = new WorkerMessenger({
                        executionControllerUrl: `http://localhost:${port}`,
                        workerId,
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
                                time: _.times(opCount, () => _.random(0, 2000)),
                                size: _.times(opCount, () => _.random(0, 100)),
                                memory: _.times(opCount, () => _.random(0, 10000)),
                            };
                        }

                        if (sliceFailed) {
                            msg.error = 'Oh no, slice failure';
                            await stateStore.updateState(slice, 'error', msg.error);
                        } else {
                            await stateStore.updateState(slice, 'completed');
                        }

                        async function completeSlice() {
                            if (!pauseAndResume) {
                                await Promise.delay(0);
                                await workerMessenger.sliceComplete(msg);
                                return;
                            }

                            await Promise.all([
                                clusterMaster.pauseExecution(nodeId, exId)
                                    .then(() => clusterMaster.resumeExecution(nodeId, exId)),
                                Promise.delay(100)
                                    .then(() => workerMessenger.sliceComplete(msg)),
                            ]);
                        }

                        await Promise.all([
                            waitForReconnect(),
                            completeSlice(),
                        ]);

                        await process();
                    }

                    await process();

                    await workerMessenger.shutdown();
                }

                function startWorkers() {
                    return Promise.all(_.times(workers, startWorker));
                }

                if (!_.isEmpty(emitsExecutionUpdate)) {
                    setImmediate(() => {
                        if (!exController) return;

                        exController.events.emit('slicer:execution:update', {
                            update: emitsExecutionUpdate
                        });
                    });
                }

                if (emitSlicerRangeExpansion) {
                    setImmediate(() => {
                        if (!exController) return;
                        exController.events.emit('slicer:slice:range_expansion');
                    });
                }

                if (emitSlicerRecursion) {
                    setImmediate(() => {
                        if (!exController) return;
                        exController.events.emit('slicer:slice:recursion');
                    });
                }

                const requestAnayltics = setTimeout(async () => {
                    try {
                        await clusterMaster.requestAnalytics(nodeId, exId);
                    } catch (err) {
                        // it shouldn't matter
                    }
                }, 100);

                testContext.attachCleanup(() => clearTimeout(requestAnayltics));

                await Promise.all([
                    startWorkers(),
                    exController.run(),
                ]);

                clearTimeout(requestAnayltics);
            });

            afterEach(() => testContext.cleanup());

            it('should process the execution correctly correctly', async () => {
                const { ex_id: exId } = testContext.executionContext;
                const clusterAnalytics = testContext.clusterMaster.getClusterAnalytics();
                expect(clusterAnalytics).not.toEqual(defaultClusterAnalytics);

                expect(slices).toBeArrayOfSize(count);
                _.times(count, (i) => {
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

                if (!_.isEmpty(emitsExecutionUpdate)) {
                    expect(exStatus).toHaveProperty('operations', emitsExecutionUpdate);
                }

                if (emitSlicerRangeExpansion) {
                    expect(exStatus._slicer_stats).toHaveProperty('slice_range_expansion', 1);
                }

                if (emitSlicerRecursion) {
                    expect(exStatus._slicer_stats).toHaveProperty('subslices', 1);
                }
            });
        });
    });

    describe('when testing shutdown', () => {
        let testContext;
        let exController;

        beforeEach(async () => {
            testContext = new TestContext({
                assignment: 'execution_controller',
            });

            await testContext.initialize();

            exController = new ExecutionController(
                testContext.context,
                testContext.executionContext
            );
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

                    exController.executionAnalytics = {};
                    exController.executionAnalytics.shutdown = () => Promise.reject(new Error('Execution Analytics Error'));

                    exController.clusterMasterClient = {};
                    exController.clusterMasterClient.shutdown = () => Promise.reject(new Error('Cluster Master Client Error'));

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
                        expect(errMsg).toInclude('Execution Analytics Error');
                        expect(errMsg).toInclude('Cluster Master Client Error');
                        expect(errMsg).toInclude('Messenger Error');
                    }
                });
            });
        });
    });
});
