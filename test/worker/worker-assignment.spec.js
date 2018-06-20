'use strict';

const Worker = require('../..');

describe('Worker Assignment', () => {
    let worker;
    beforeEach(() => {
        worker = new Worker({
            assignment: 'worker',
            job: {
                example: true
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        });
    });

    describe('->start', () => {
        beforeEach(() => worker.start());

        it('should have the method', () => { });
    });
});
