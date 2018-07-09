'use strict';

const { ExecutionController } = require('../..');
const { TestContext } = require('../helpers');

describe('ExecutionController', () => {
    let exController;
    let testContext;

    beforeEach(() => {
        testContext = new TestContext('execution_controller', {
            assignment: 'execution_controller'
        });
        exController = new ExecutionController(testContext.context, testContext.jobConfig);
        testContext.attachCleanup(() => exController.shutdown());
    });

    afterEach(() => testContext.cleanup());

    it('should the correct methods', () => {
        expect(exController).toHaveProperty('initialize');
        expect(exController.initialize).toBeFunction();

        expect(exController).toHaveProperty('run');
        expect(exController.run).toBeFunction();

        expect(exController).toHaveProperty('shutdown');
        expect(exController.shutdown).toBeFunction();
    });

    describe('when running', () => {
        beforeEach(async () => {
            await exController.initialize();
            await exController.run();
        });

        it('should have the stores', () => {
            expect(exController.stores).toHaveProperty('stateStore');
            expect(exController.stores.stateStore).toHaveProperty('shutdown');
            expect(exController.stores).toHaveProperty('exStore');
            expect(exController.stores.exStore).toHaveProperty('shutdown');
        });
    });
});
