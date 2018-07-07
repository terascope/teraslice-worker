'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
    let exController;
    let testContext;

    beforeAll(() => {
        testContext = new TestContext('execution_controller', {
            assignment: 'execution_controller'
        });
        exController = new ExecutionController(testContext.context, testContext.jobConfig);
        testContext.attachCleanup(() => exController.shutdown());
    });

    afterAll(() => testContext.cleanup());

    it('should the correct methods', () => {
        expect(exController).toHaveProperty('start');
        expect(exController.start).toBeFunction();
        expect(exController).toHaveProperty('shutdown');
        expect(exController.shutdown).toBeFunction();
    });

    describe('when started', () => {
        beforeAll(async () => {
            await exController.initialize();
            await exController.start();
        });

        it('should have the stores', () => {
            expect(exController.stores).toHaveProperty('stateStore');
            expect(exController.stores.stateStore).toHaveProperty('shutdown');
            expect(exController.stores).toHaveProperty('exStore');
            expect(exController.stores.exStore).toHaveProperty('shutdown');
        });
    });
});
