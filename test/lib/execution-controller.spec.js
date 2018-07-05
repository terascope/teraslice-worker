'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
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

    describe('when started', () => {
        beforeEach(() => exController.start());
        it('should have the stores', () => {
            expect(exController.stores.stateStore).toBeDefined();
            expect(exController.stores.stateStore).toHaveProperty('destroy');
            expect(exController.stores.analyticsStore).toBeDefined();
            expect(exController.stores.analyticsStore).toHaveProperty('destroy');
        });
    });
});
