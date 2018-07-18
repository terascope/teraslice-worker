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
                slicerResults: [
                    { example: 'single-slice' },
                    null
                ],
                body: { example: 'single-slice' },
                count: 1,
                lifecycle: 'once'
            }
        ],
        // message, config
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
                lifecycle: 'once'
            }
        ]
    ];

    describe.each(testCases)('when processing %s', (m, options) => {
        const {
            slicerResults,
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
                slicerResults,
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

            await Promise.all([
                Promise.mapSeries(times(count), async () => {
                    const startTime = Date.now();
                    const slice = await workerMessenger.waitForSlice(() => {
                        const elapsed = Date.now() - startTime;
                        return elapsed > 5000;
                    });
                    workerMessenger.sliceComplete({ slice });
                    return slice;
                }),
                exController.run()
            ]).spread((_slices) => {
                slices = _slices;
            });
        });

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
});
