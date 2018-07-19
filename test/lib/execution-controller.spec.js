'use strict';

const times = require('lodash/times');
const random = require('lodash/random');
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
                body: { example: 'single-slice' },
                count: 1,
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
                lifecycle: 'once',
                analytics: true,
            }
        ],
    ];

    describe.each(testCases)('when processing %s', (m, options) => {
        const {
            slicerResults,
            slicerQueueLength,
            count,
            lifecycle,
            body,
            reconnect,
            analytics
        } = options;

        let exController;
        let testContext;
        let workerMessenger;
        let slices;

        beforeEach(async () => {
            slices = [];
            const port = await findPort();

            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
                slicerPort: port,
                slicerQueueLength,
                slicerResults,
                lifecycle,
                analytics,
            });

            await testContext.makeItARealJob();

            exController = new ExecutionController(testContext.context, testContext.jobConfig);
            const {
                network_latency_buffer: networkerLatencyBuffer,
                action_timeout: actionTimeout,
            } = testContext.context.sysconfig.teraslice;

            workerMessenger = new WorkerMessenger({
                executionControllerUrl: `http://localhost:${port}`,
                workerId: newId('worker'),
                networkerLatencyBuffer,
                actionTimeout,
                socketOptions: {
                    timeout: 1000,
                    reconnection: true,
                    reconnectionAttempts: 2,
                    reconnectionDelay: 10,
                    reconnectionDelayMax: 100
                }
            });

            testContext.attachCleanup(() => workerMessenger.shutdown());

            testContext.attachCleanup(() => exController.shutdown());

            await exController.initialize();

            await workerMessenger.start();
            await workerMessenger.ready();

            workerMessenger.available = true;
            workerMessenger.on('slicer:slice:new', (slice) => {
                // if (reconnect) {
                //     workerMessenger.manager.reconnect();
                // }

                if (analytics) {
                    const analyticsData = {
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
                    workerMessenger.sliceComplete({
                        slice,
                        analytics: analyticsData
                    });
                } else {
                    workerMessenger.sliceComplete({ slice });
                }

                slices.push(slice);
            });

            await exController.run();
        }, 15 * 1000);

        afterEach(() => testContext.cleanup());

        it('should send the slices to the worker', () => {
            expect(slices).toBeArrayOfSize(count);
            times(count, (i) => {
                const slice = slices[i];
                expect(slice).toHaveProperty('request');
                expect(slice.request).toEqual(body);
            });

            const { exId } = testContext;
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
