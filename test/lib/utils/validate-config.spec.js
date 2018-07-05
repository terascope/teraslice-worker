'use strict';

const { validateJobConfig } = require('../../../lib/utils');

describe('Worker Config Validation', () => {
    describe('when constructed nothing', () => {
        it('should throw an error', () => {
            expect(() => validateJobConfig()).toThrow('Worker requires a valid job configuration');
        });
    });

    describe('when constructed without a assignment', () => {
        it('should throw an error', () => {
            expect(() => validateJobConfig({ example: true })).toThrow('Job configuration requires a valid assignment');
        });
    });

    describe('when constructed with a invalid assignment', () => {
        it('should throw an error', () => {
            expect(() => validateJobConfig({ assignment: 'invalid' })).toThrow('Job configuration requires assignment to worker, or execution_controller');
        });
    });

    describe('when constructed without a job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'worker',
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid job');
        });
    });

    describe('when constructed without a valid job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'worker',
                job: 'this-should-fail'
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid');
        });
    });

    describe('when constructed without a valid ex_id', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'worker',
                job: { hello: true },
                ex_id: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid ex_id');
        });
    });

    describe('when constructed without a valid job_id', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'worker',
                job: { hello: true },
                ex_id: 'example',
                job_id: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid job_id');
        });
    });

    describe('when constructed without a valid slicer_port', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'worker',
                job: { hello: true },
                ex_id: 'example',
                job_id: 'example',
                slicer_port: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid slicer_port');
        });
    });

    describe('when constructed valid input', () => {
        it('should not throw an error', () => {
            const config = {
                assignment: 'worker',
                job: { hello: true },
                ex_id: 'example',
                job_id: 'example',
                slicer_port: 1234
            };
            expect(() => validateJobConfig(config)).not.toThrow();
        });
    });
});
