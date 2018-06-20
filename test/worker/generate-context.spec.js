'use strict';

const { generateContext } = require('../../');

describe('Terafoundation Context', () => {
    let context;

    beforeEach(() => {
        context = generateContext();
    });

    it('should return have the correct apis', () => {
        expect(context.apis.foundation).toHaveProperty('makeLogger');
        expect(context.foundation).toHaveProperty('makeLogger');
        expect(context.apis.foundation).toHaveProperty('getSystemEvents');
        expect(context.foundation).toHaveProperty('getEventEmitter');
        expect(context.apis.foundation).toHaveProperty('getConnection');
        expect(context.foundation).toHaveProperty('getConnection');
        expect(context.apis.foundation).not.toHaveProperty('startWorkers');
        expect(context.foundation).not.toHaveProperty('startWorkers');
        expect(context.apis).toHaveProperty('registerAPI');
    });
});

