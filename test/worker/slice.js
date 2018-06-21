'use strict';

const Slice = require('../../lib/worker/slice');

describe('Slice', () => {
    it('should not throw an error if constructed', () => {
        expect(() => new Slice()).not.toThrow();
    });
});

