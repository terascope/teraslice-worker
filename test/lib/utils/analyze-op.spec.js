'use strict';

const { analyzeOp } = require('../../../lib/utils');

describe('analyzeOp', () => {
    it('should not throw an error if constructed', () => {
        expect(() => new analyzeOp()).not.toThrow();
    });
});

