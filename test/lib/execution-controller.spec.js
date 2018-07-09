'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
    describe('when the slicer returns an array of slices', () => {
        let exController;
        let testContext;

        beforeEach(async () => {
            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller'
            });

            exController = new ExecutionController(testContext.context, testContext.jobConfig);

            testContext.attachCleanup(() => exController.shutdown());

            await exController.initialize();
            await exController.runOnce();
        });

        afterEach(() => testContext.cleanup());

        it('should enqueue the slices', () => {
            expect(exController.slicerQueue.size()).toEqual(10);

            const { exId } = testContext;
            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(10);
        });
    });

    describe('when the slicer returns a single', () => {
        let exController;
        let testContext;

        beforeEach(async () => {
            testContext = new TestContext('execution_controller', {
                assignment: 'execution_controller',
            });

            testContext.exampleReader.slicer.mockResolvedValue({ example: 'howdy' });

            exController = new ExecutionController(testContext.context, testContext.jobConfig);

            testContext.attachCleanup(() => exController.shutdown());

            await exController.initialize();
            await exController.runOnce();
        });

        afterEach(() => testContext.cleanup());

        it('should enqueue the slices', () => {
            expect(testContext.exampleReader.slicer).toHaveBeenCalled();
            expect(exController.slicerQueue.size()).toEqual(1);

            const { exId } = testContext;
            const { stateStore } = exController.stores;
            const query = `ex_id:${exId} AND state:start`;
            return expect(stateStore.count(query, 0)).resolves.toEqual(1);
        });
    });
});
