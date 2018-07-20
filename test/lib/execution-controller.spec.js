'use strict';

const times = require('lodash/times');
const random = require('lodash/random');
const { EventEmitter } = require('events');
const { ExecutionController } = require('../..');
const WorkerMessenger = require('../../lib/messenger/worker');
const { TestContext, findPort, newId } = require('../helpers');


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
                reconnect: false,
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
                reconnect: false,
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
                reconnect: true,
                body: { example: 'slice-failure' },
                count: 1,
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
                    null
                ],
                reconnect: true,
                slicerQueueLength: 'QUEUE_MINIMUM_SIZE',
                body: { example: 'slice-dynamic' },
                count: 1,
                workers: 1,
                lifecycle: 'once',
                analytics: true,
            }
        ],
        [
            'a slice that fails',
            {
                slicerResults: [
                    { example: 'slice-fail' },
                    null
                ],
                reconnect: true,
                failSlice: true,
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
            failSlice,
        } = options;

        let exController;
        let testContext;
        let slices;
        let exStatus;

        beforeEach(async () => {
            slices = [];
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

            exController = new ExecutionController(testContext.context, testContext.jobConfig);
            const {
                network_latency_buffer: networkerLatencyBuffer,
                action_timeout: actionTimeout,
            } = testContext.context.sysconfig.teraslice;

            testContext.attachCleanup(() => exController.shutdown());

            await exController.initialize();
            const doneProcessing = () => slices.length >= count;

            async function startWorker() {
                const workerMessenger = new WorkerMessenger({
                    executionControllerUrl: `http://localhost:${port}`,
                    workerId: newId('worker'),
                    networkerLatencyBuffer,
                    actionTimeout,
                    events: new EventEmitter(),
                    socketOptions: {
                        timeout: 1000,
                        reconnection: true,
                        reconnectionAttempts: 2,
                        reconnectionDelay: 10,
                        reconnectionDelayMax: 100
                    }
                });

                testContext.attachCleanup(() => workerMessenger.shutdown());

                await workerMessenger.start();
                await workerMessenger.ready();

                async function process() {
                    if (doneProcessing()) return;

                    const slice = await workerMessenger.waitForSlice(doneProcessing);

                    if (!slice) return;

                    slices.push(slice);

                    if (reconnect) {
                        workerMessenger.manager.reconnect();
                    }

                    const msg = {
                        slice,
                    };

                    if (analytics) {
                        msg.analyticsData = {
                            time: [
                                random(0, 2000),
                                random(0, 2000),
                            ],
                            size: [
                                random(0, 100),
                                random(0, 100)
                            ],
                            memory: [
                                random(0, 10000),
                                random(0, 10000)
                            ]
                        };
                    }

                    if (failSlice) {
                        msg.error = 'Oh no, slice failure';
                    }

                    workerMessenger.sliceComplete(msg);

                    await process();
                }

                await process();
            }

            function startWorkers() {
                return Promise.all(times(workers, startWorker));
            }

            await Promise.all([
                startWorkers(),
                exController.run(),
            ]);

            exStatus = await testContext.getExStatus();
        });

        afterEach(() => testContext.cleanup());

        it('should process the slices correctly', () => {
            const { exId } = testContext;

            expect(slices).toBeArrayOfSize(count);
            times(count, (i) => {
                const slice = slices[i];
                expect(slice).toHaveProperty('request');
                expect(slice.request).toEqual(body);
            });

            if (failSlice) {
                expect(exStatus).toHaveProperty('_failureReason', `execution: ${exId} had 1 slice failures during processing`);
                expect(exStatus).toHaveProperty('_has_errors', true);
                expect(exStatus).toHaveProperty('_status', 'failed');
            } else {
                expect(exStatus).toHaveProperty('_status', 'completed');
            }

            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(count);
        });
    });

    describe('when testing shutdown', () => {
        let testContext;
        let exController;

        beforeEach(() => {
            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
            });
            exController = new ExecutionController(testContext.context, testContext.jobConfig);
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
