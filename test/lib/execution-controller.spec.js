'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
    let exController;
    let testContext;

    beforeEach(async () => {
        testContext = new TestContext('execution_controller', {
            assignment: 'execution_controller'
        });
        exController = new ExecutionController(testContext.context, testContext.jobConfig);

        testContext.attachCleanup(() => exController.shutdown());

        await exController.initialize();
    });

    afterEach(() => testContext.cleanup());

    it('should an array of slicers', () => {
        expect(exController.slicers).toBeArray();
    });

    describe('when running a slice', () => {
        beforeEach(async () => {
            await exController.runOnce();
        });

        it('should enqueue the slices', () => {
            expect(exController.slicerQueue.size()).toEqual(10);

            const { exId } = testContext;
            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(10);
        });
    });
});
