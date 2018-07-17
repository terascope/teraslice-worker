'use strict';

const times = require('lodash/times');
const { ExecutionController } = require('../..');
const WorkerMessenger = require('../../lib/messenger/worker');
const { TestContext, findPort, newId } = require('../helpers');


describe('ExecutionController', () => {
    const testCases = [
        // message, config
        [
            'one slice',
            {
                newSlicer: jest.fn(() => {
                    let firedOnce = false;
                    return [
                        () => {
                            if (firedOnce) {
                                return null;
                            }
                            firedOnce = true;
                            return { example: 'single-slice' };
                        }
                    ];
                }),
                body: { example: 'single-slice' },
                count: 1,
                lifecycle: 'once'
            }
        ],
        // message, config
        [
            'sub-slices',
            {
                newSlicer: jest.fn(() => {
                    let firedOnce = false;
                    return [
                        () => {
                            if (firedOnce) {
                                return null;
                            }
                            firedOnce = true;
                            return [
                                { example: 'subslice' },
                                { example: 'subslice' },
                                { example: 'subslice' }
                            ];
                        }
                    ];
                }),
                count: 3,
                body: { example: 'subslice' },
                lifecycle: 'once'
            }
        ]
    ];

    describe.each(testCases)('when processing %s', (m, options) => {
        const {
            newSlicer,
            count,
            lifecycle,
            body
        } = options;

        let exController;
        let testContext;
        let workerMessenger;
        let slices;

        beforeAll(async () => {
            await TestContext.cleanupAll();
        });

        beforeEach(async () => {
            slices = null;
            const port = await findPort();

            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
                slicerPort: port,
                lifecycle,
            });

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
                    reconnection: false,
                }
            });

            testContext.attachCleanup(() => workerMessenger.close());

            testContext.attachCleanup(() => exController.shutdown());

            await exController.initialize();

            exController.executionContext.slicer.newSlicer = newSlicer;

            await workerMessenger.start();
            await workerMessenger.ready();

            await Promise.all([
                Promise.all(times(count, () => {
                    const startTime = Date.now();
                    return workerMessenger.waitForSlice(() => {
                        const elapsed = Date.now() - startTime;
                        return elapsed > 5000;
                    });
                })),
                exController.run()
            ]).spread((_slices) => {
                slices = _slices;
            });
        });

        afterEach(() => testContext.cleanup());

        it('should send the slices to the worker', () => {
            times(count, (i) => {
                const slice = slices[i];
                expect(slice.request).toEqual(body);
            });

            const { exId } = testContext;
            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(count);
        });
    });
});
