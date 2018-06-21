'use strict';

const { validateJobConfig } = require('../../');

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

    describe('when constructed without a job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid job');
        });
    });

    describe('when constructed without a valid job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: 'this-should-fail'
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid');
        });
    });

    describe('when constructed without a valid exId', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: { hello: true },
                exId: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid exId');
        });
    });

    describe('when constructed without a valid jobId', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: { hello: true },
                exId: 'example',
                jobId: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid jobId');
        });
    });

    describe('when constructed without a valid slicerPort', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: { hello: true },
                exId: 'example',
                jobId: 'example',
                slicerPort: null
            };
            expect(() => validateJobConfig(config)).toThrow('Job configuration requires a valid slicerPort');
        });
    });

    describe('when constructed valid input', () => {
        it('should not throw an error', () => {
            const config = {
                assignment: 'example',
                job: { hello: true },
                exId: 'example',
                jobId: 'example',
                slicerPort: 1234
            };
            expect(() => validateJobConfig(config)).not.toThrow();
        });
    });
});
