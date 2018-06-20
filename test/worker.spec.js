'use strict';

const { Worker } = require('../');

describe('Worker', () => {
    describe('when constructed nothing', () => {
        test('should throw an error', () => {
            expect(() => new Worker()).toThrow('Worker requires a valid configuration');
        });
    });

    describe('when constructed without a assignment', () => {
        test('should throw an error', () => {
            expect(() => new Worker({ example: true })).toThrow('Worker configuration requires a valid assignment');
        });
    });

    describe('when constructed without a job', () => {
        test('should throw an error', () => {
            const config = {
                assignment: 'example',
            };
            expect(() => new Worker(config)).toThrow('Worker configuration requires a valid job');
        });
    });

    describe('when constructed without a valid job', () => {
        test('should throw an error', () => {
            const config = {
                assignment: 'example',
                job: 'this-should-fail'
            };
            expect(() => new Worker(config)).toThrow('Worker configuration requires a valid');
        });
    });

    describe('given a "worker" assignment', () => {
        let worker;
        beforeEach(() => {
            worker = new Worker({
                assignment: 'worker',
                job: {
                    example: true
                }
            });
        });

        describe('->start', () => {
            beforeEach(() => worker.start());

            test('should have the method', () => { });
        });
    });
});
