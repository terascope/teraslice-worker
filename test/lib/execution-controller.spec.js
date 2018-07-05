'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
    describe('when constructed', () => {
        let exController;
        let testContext;

        beforeEach(() => {
            testContext = new TestContext('worker');
            exController = new ExecutionController(testContext.context, testContext.jobConfig);
        });

        afterEach(async () => {
            await exController.shutdown();
            await testContext.cleanup();
        });

        it('should the correct methods', () => {
            expect(exController).toHaveProperty('start');
            expect(exController.start).toBeFunction();
            expect(exController).toHaveProperty('shutdown');
            expect(exController.shutdown).toBeFunction();
        });
    });
});
