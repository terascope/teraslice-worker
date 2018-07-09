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

        it('should have a length of 10 slices', () => {
            expect(exController.slicerQueue.size()).toEqual(10);
        });
    });
});
