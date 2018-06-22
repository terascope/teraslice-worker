'use strict';

const get = require('lodash/get');
const Job = require('../../../lib/job');
const testContext = require('../../helpers/test-context');

describe('Worker Job', () => {
    let job;
    let clusterName; // eslint-disable-line no-unused-vars
    let jobConfig;
    beforeEach(() => {
        const { context, _clusterName } = testContext();
        clusterName = _clusterName;
        jobConfig = {
            type: 'example',
            job: {
                example: true,
                operations: [
                    {
                        _op: 'example-op-one',
                    },
                    {
                        _op: 'example-op-two',
                    }
                ]
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        };
        job = new Job(context, jobConfig);
    });

    it('should throw an error if reporters are specified', () => {
        const { context } = testContext();
        context.sysconfig.teraslice.reporter = true;
        expect(() => {
            new Job(context, { hello: true }); // eslint-disable-line no-new
        }).toThrowError('reporters are not functional at this time, please do not set one in the configuration');
    });

    it('should have a method initialize', () => {
        expect(typeof job.initialize).toEqual('function');
    });

    it('should register the api job_runner', () => {
        const jobRunnerApis = get(job, 'context.apis.job_runner');
        expect(jobRunnerApis).toHaveProperty('getOpConfig');
        expect(typeof jobRunnerApis.getOpConfig).toEqual('function');
    });

    it('getOpConfig should return the matching op', () => {
        expect(job.getOpConfig('example-op-one')).toEqual({
            _op: 'example-op-one',
        });

        const getOpConfig = get(job, 'context.apis.job_runner.getOpConfig');
        expect(getOpConfig('example-op-one')).toEqual({
            _op: 'example-op-one',
        });
    });

    it('getOpConfig should return nothing if none found', () => {
        expect(job.getOpConfig('this-op-does-not-exist')).not.toBe(expect.anything());

        const getOpConfig = get(job, 'context.apis.job_runner.getOpConfig');
        expect(getOpConfig('this-op-does-not-exist')).not.toBe(expect.anything());
    });

    it('should create an opRunner', () => {
        expect(job).toHaveProperty('opRunner');
        expect(job.opRunner).toHaveProperty('load');
    });

    describe('->initialize', () => {
        let executionApi;

        beforeEach(async () => {
            executionApi = await job.initialize();
        });

        it('should resolve an execution api', () => {
            expect(executionApi).toEqual(expect.objectContaining({
                reader: expect.any(Function),
                config: expect.any(Object),
                queue: expect.any(Array)
            }));
            expect(executionApi.reader).toEqual(executionApi.queue[0]);
            expect(executionApi.config).toEqual(jobConfig.job);
            expect(executionApi.reporter).not.toBe(expect.anything());
            expect(executionApi.slicer).not.toBe(expect.anything());
        });
    });
});

