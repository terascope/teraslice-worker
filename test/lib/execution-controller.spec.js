'use strict';

const { ExecutionController } = require('../..');
const WorkerMessenger = require('../../lib/messenger/worker');
const { TestContext, findPort, newId } = require('../helpers');

describe('ExecutionController', () => {
    const slices = [
        [[{ example: 'howdy' }], 1],
        [{ example: 'howdy' }, 1]
    ];

    describe.each(slices)('when the slicer returns %j', (sliceData, count) => {
        let exController;
        let testContext;
        let workerMessenger;
        let slice;

        beforeEach(async () => {
            const port = await findPort();

            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
                slicerPort: port,
            });

            testContext.exampleReader.slicer.mockResolvedValue(sliceData);

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
            const waitForSlice = workerMessenger.waitForSlice(() => {
                const elapsed = Date.now() - startTime;
                return elapsed > 5000;
            });

            await exController.runOnce();
            slice = await waitForSlice;
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
