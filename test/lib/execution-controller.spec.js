'use strict';

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
                            return { example: 'howdy' };
                        }
                    ];
                }),
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
                                { example: 'howdy' },
                                { example: 'howdy' },
                                { example: 'howdy' }
                            ];
                        }
                    ];
                }),
                count: 3,
                lifecycle: 'once'
            }
        ]
    ];

    describe.each(testCases)('when processing %s', (m, { newSlicer, count, lifecycle }) => {
        let exController;
        let testContext;
        let workerMessenger;
        let slice;

        beforeAll(() => TestContext.cleanupAll(true));
        beforeEach(async () => {
            const port = await findPort();

            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
                slicerPort: port,
                newSlicer,
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

            await workerMessenger.start();
            await workerMessenger.ready();

            const startTime = Date.now();

            await Promise.all([
                workerMessenger.waitForSlice(() => {
                    const elapsed = Date.now() - startTime;
                    return elapsed > 5000;
                }),
                exController.run()
            ]).spread((_slice) => {
                slice = _slice;
            });
        });

        afterEach(() => testContext.cleanup());

        it('should send the slices to the worker', () => {
            expect(slice.request).toEqual({ example: 'howdy' });

            const { exId } = testContext;
            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(count);
        });
    });
});
