'use strict';

const get = require('lodash/get');
const path = require('path');
const Job = require('../../../lib/job');
const testContext = require('../../helpers/test-context');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReaderMock = require(path.join(opsPath, 'example-reader')).newReader;
const exampleOpMock = require(path.join(opsPath, 'example-op')).newProcessor;

describe('Worker Job', () => {
    describe('when constructing', () => {
        let job;
        let jobConfig;
        beforeEach(() => {
            const { context } = testContext();
            jobConfig = {
                type: 'example',
                job: {
                    example: true,
                    assets: [],
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
            expect(job.initialize).toBeFunction();
        });

        it('should register the api job_runner', () => {
            const jobRunnerApis = get(job, 'context.apis.job_runner');
            expect(jobRunnerApis).toHaveProperty('getOpConfig');
            expect(jobRunnerApis.getOpConfig).toBeFunction();
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
            expect(job.getOpConfig('this-op-does-not-exist')).not.toBeEmpty();

            const getOpConfig = get(job, 'context.apis.job_runner.getOpConfig');
            expect(getOpConfig('this-op-does-not-exist')).not.toBeEmpty();
        });

        it('should create an opRunner', () => {
            expect(job).toHaveProperty('loadOp');
            expect(typeof job.loadOp).toBe('function');
        });
    });

    describe('->initialize', () => {
        describe('when analyitics is not enabled', () => {
            let executionApi;
            let job;
            let jobConfig;
            let context;
            beforeEach(() => {
                const { _context } = testContext('worker-job');
                context = _context;
                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: [],
                        operations: [
                            {
                                _op: path.join(opsPath, 'example-reader'),
                                exampleProp: 321
                            },
                            {
                                _op: path.join(opsPath, 'example-op'),
                                exampleProp: 123
                            }
                        ]
                    },
                    exId: 'example-ex-id',
                    jobId: 'example-job-id'
                };
                job = new Job(context, jobConfig);
                exampleReaderMock.mockResolvedValue(jest.fn());
                exampleOpMock.mockResolvedValue(jest.fn());

                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeEmpty();
                }).catch((err) => {
                    expect(err).toBeEmpty();
                });
            });

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeEmpty();
                const {
                    queue,
                    config,
                    reader,
                    reporter,
                    slicer
                } = executionApi;
                expect(queue).toBeArrayOfSize(2);
                expect(queue[0]).toBeFunction();
                expect(queue[1]).toBeFunction();
                expect(reader).toEqual(queue[0]);
                expect(config).toEqual(jobConfig.job);
                expect(reporter).not.toBeEmpty();
                expect(slicer).not.toBeEmpty();
            });

            it('should load the ops', () => {
                expect(exampleReaderMock).toHaveBeenCalledTimes(1);
                expect(exampleReaderMock).toHaveBeenCalledWith(context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, jobConfig.job);
                expect(exampleOpMock).toHaveBeenCalledTimes(1);
                expect(exampleOpMock).toHaveBeenCalledWith(context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, jobConfig.job);
            });
        });

        describe('when analyitics is enabled', () => {
            let executionApi;
            let job;
            let jobConfig;
            let context;
            beforeEach(() => {
                const { _context } = testContext('worker-job');
                context = _context;
                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: [],
                        analyitics: true,
                        operations: [
                            {
                                _op: path.join(opsPath, 'example-reader'),
                                exampleProp: 321
                            },
                            {
                                _op: path.join(opsPath, 'example-op'),
                                exampleProp: 123
                            }
                        ]
                    },
                    exId: 'example-ex-id',
                    jobId: 'example-job-id'
                };
                job = new Job(context, jobConfig);
                exampleReaderMock.mockResolvedValue(jest.fn());
                exampleOpMock.mockResolvedValue(jest.fn());

                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeEmpty();
                }).catch((err) => {
                    expect(err).toBeEmpty();
                });
            });

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeEmpty();
                const {
                    queue,
                    config,
                    reader,
                    reporter,
                    slicer
                } = executionApi;
                expect(queue).toBeArrayOfSize(2);
                expect(queue[0]).toBeFunction();
                expect(queue[1]).toBeFunction();
                expect(reader).toEqual(queue[0]);
                expect(config).toEqual(jobConfig.job);
                expect(reporter).not.toBeEmpty();
                expect(slicer).not.toBeEmpty();
            });

            it('should load the ops', () => {
                expect(exampleReaderMock).toHaveBeenCalledTimes(1);
                expect(exampleReaderMock).toHaveBeenCalledWith(context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, jobConfig.job);
                expect(exampleOpMock).toHaveBeenCalledTimes(1);
                expect(exampleOpMock).toHaveBeenCalledWith(context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, jobConfig.job);
            });
        });
    });
});

