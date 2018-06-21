'use strict';

const Job = require('../../../lib/job');

describe('Job', () => {
    let job;
    beforeEach(() => {
        job = new Job();
    });

    it('should have a method initialize', () => {
        expect(typeof job.initialize).toEqual('function');
    });
});

