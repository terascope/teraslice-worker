'use strict';

const { Worker } = require('../');

describe('Worker', () => {
    let worker;
    beforeEach(() => {
        worker = new Worker();
    });

    test('should return a instance of the worker', () => {
        expect(worker instanceof Worker).toBe(true);
    });

    test('should have a method start', () => {
        expect(typeof worker.start).toEqual('function');
    });
});

