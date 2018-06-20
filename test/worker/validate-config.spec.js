'use strict';

const { validateConfig } = require('../../').Worker;

describe('Worker Config Validation', () => {
    describe('when constructed nothing', () => {
        it('should throw an error', () => {
            expect(() => validateConfig()).toThrow('Worker requires a valid configuration');
        });
    });

    describe('when constructed without a assignment', () => {
        it('should throw an error', () => {
            expect(() => validateConfig({ example: true })).toThrow('Worker configuration requires a valid assignment');
        });
    });

    describe('when constructed without a job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
            };
            expect(() => validateConfig(config)).toThrow('Worker configuration requires a valid job');
        });
    });

    describe('when constructed without a valid job', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: 'this-should-fail'
            };
            expect(() => validateConfig(config)).toThrow('Worker configuration requires a valid');
        });
    });

    describe('when constructed without a valid exId', () => {
        it('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: { hello: true },
                exId: null
            };
            expect(() => validateConfig(config)).toThrow('Worker configuration requires a valid exId');
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
            expect(() => validateConfig(config)).toThrow('Worker configuration requires a valid jobId');
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
            expect(() => validateConfig(config)).toThrow('Worker configuration requires a valid slicerPort');
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
            expect(() => validateConfig(config)).not.toThrow();
        });
    });
});
