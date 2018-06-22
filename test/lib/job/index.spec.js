'use strict';

const get = require('lodash/get');
const Job = require('../../../lib/job');
const testContext = require('../../helpers/test-context');

describe('Job', () => {
    let job;
    let clusterName; // eslint-disable-line no-unused-vars
    beforeEach(() => {
        const { context, _clusterName } = testContext();
        clusterName = _clusterName;
        const jobConfig = {
            type: 'example',
            job: {
                example: true
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        };
        job = new Job(context, jobConfig);
    });

    it('should have a method initialize', () => {
        expect(typeof job.initialize).toEqual('function');
    });

    it('should register the api job_runner', () => {
        const jobRunnerApis = get(job, 'context.apis.job_runner');
        expect(jobRunnerApis).toHaveProperty('getOpConfig');
        expect(typeof jobRunnerApis.getOpConfig).toEqual('function');
    });
});

