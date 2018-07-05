'use strict';

const path = require('path');
const get = require('lodash/get');
const Job = require('../../lib/job');
const { TestContext, opsPath } = require('../helpers');

const exampleAssetDir = path.join(opsPath, 'example-asset');

describe('Job (assignment "worker")', () => {
    describe('when constructing', () => {
        let job;
        let testContext;

        beforeEach(() => {
            testContext = new TestContext('worker-job', {
                operations: [
                    {
                        _op: 'example-op-one',
                    },
                    {
                        _op: 'example-op-two',
                    }
                ],
            });
            testContext.jobConfig.job.example = true;
            job = new Job(testContext.context, testContext.jobConfig);
        });

        afterEach(() => testContext.cleanup());

        it('should throw an error if reporters are specified', () => {
            testContext.context.sysconfig.teraslice.reporter = true;
            expect(() => {
                new Job(testContext.context, { hello: true }); // eslint-disable-line no-new
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
            expect(job.getOpConfig('this-op-does-not-exist')).toBeNil();

            const getOpConfig = get(job, 'context.apis.job_runner.getOpConfig');
            expect(getOpConfig('this-op-does-not-exist')).toBeNil();
        });

        it('should create an opRunner', () => {
            expect(job).toHaveProperty('opRunner');
            expect(job.opRunner.findOp).toBeFunction();
        });
    });

    describe('->initialize', () => {
        describe('when op name is not a string', () => {
            let job;
            let testContext;
            beforeEach(() => {
                testContext = new TestContext('worker-job:fail', {
                    operations: [
                        {
                            _op: null,
                        }
                    ]
                });
                job = new Job(testContext.context, testContext.jobConfig);
            });

            afterEach(() => testContext.cleanup());

            it('should reject with an error', () => {
                const errMsg = 'please verify that ops_directory in config and _op for each job operations are strings';
                return expect(job.initialize()).rejects.toThrow(errMsg);
            });
        });

        describe('when analytics is not enabled', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job', {
                    operations: [
                        {
                            _op: path.join(opsPath, 'example-reader'),
                            exampleProp: 321
                        },
                        {
                            _op: path.join(opsPath, 'example-op'),
                            exampleProp: 123
                        }
                    ],
                });

                job = new Job(testContext.context, testContext.jobConfig);

                await job.initialize();
            });

            afterEach(() => testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(job.queue).toBeArrayOfSize(2);
                expect(job.queue[0]).toBeFunction();
                expect(job.queue[1]).toBeFunction();
                expect(job.reader).toEqual(job.queue[0]);
                expect(job.config).toEqual(testContext.jobConfig.job);
                expect(job.reporter).toBeNil();
                expect(job.slicer).toBeNil();
            });

            it('should load the ops', () => {
                expect(testContext.newReader).toHaveBeenCalledTimes(1);
                expect(testContext.newReader).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, testContext.jobConfig.job);
                expect(testContext.newProcessor).toHaveBeenCalledTimes(1);
                expect(testContext.newProcessor).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, testContext.jobConfig.job);
            });
        });

        describe('when analytics is enabled', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job:analytics', {
                    analytics: true,
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
                });

                job = new Job(testContext.context, testContext.jobConfig);
                await job.initialize();
            });

            afterEach(() => testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(job.queue).toBeArrayOfSize(2);
                expect(job.queue[0]).toBeFunction();
                expect(job.queue[1]).toBeFunction();
                expect(job.reader).toEqual(job.queue[0]);
                expect(job.config).toEqual(testContext.jobConfig.job);
                expect(job.reporter).toBeNil();
                expect(job.slicer).toBeNil();
            });

            it('should load the ops', () => {
                expect(testContext.newReader).toHaveBeenCalledTimes(1);
                expect(testContext.newReader).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, testContext.jobConfig.job);
                expect(testContext.newProcessor).toHaveBeenCalledTimes(1);
                expect(testContext.newProcessor).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, testContext.jobConfig.job);
            });
        });

        describe('when using assets', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job:assets', {
                    operations: [
                        {
                            _op: 'example-asset-reader',
                        },
                        {
                            _op: 'example-asset-op',
                        }
                    ],
                    assets: ['example-asset'],
                });

                await testContext.saveAsset(exampleAssetDir);

                job = new Job(testContext.context, testContext.jobConfig);

                await job.initialize();
            });

            afterEach(() => testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(job.queue).toBeArrayOfSize(2);
                expect(job.reader).toBeFunction();
                expect(job.queue[1]).toBeFunction();
                expect(job.config).toEqual(testContext.jobConfig.job);
                expect(job.reporter).toBeNil();
                expect(job.slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await job.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await job.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets that have not been downloaded', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job:assets-download', {
                    assets: ['example-asset'],
                    operations: [
                        {
                            _op: 'example-asset-reader',
                        },
                        {
                            _op: 'example-asset-op',
                        }
                    ]
                });

                await testContext.saveAsset(exampleAssetDir, true);

                job = new Job(testContext.context, testContext.jobConfig);
                await job.initialize();
            });

            afterEach(() => testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(job.queue).toBeArrayOfSize(2);
                expect(job.reader).toBeFunction();
                expect(job.queue[1]).toBeFunction();
                expect(job.config).toEqual(testContext.jobConfig.job);
                expect(job.reporter).toBeNil();
                expect(job.slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await job.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await job.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets and they do not exist', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job:assets-fail', {
                    assets: ['missing-assets'],
                    operations: [
                        {
                            _op: 'missing-assets-reader',
                        },
                        {
                            _op: 'missing-assets-op',
                        }
                    ]
                });
                job = new Job(testContext.context, testContext.jobConfig);
            });

            afterEach(() => testContext.cleanup());

            it('should reject with a error', () => {
                const errMsg = 'asset: missing-assets was not found';
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });

        describe('when using assets and the fail on require', () => {
            let job;
            let testContext;

            beforeEach(async () => {
                testContext = new TestContext('worker-job:failing-asset', {
                    assets: ['failing-asset'],
                    operations: [
                        {
                            _op: 'failing-asset-reader',
                        }
                    ]
                });
                await testContext.saveAsset(path.join(opsPath, 'failing-asset'));
                job = new Job(testContext.context, testContext.jobConfig);
            });

            afterEach(() => testContext.cleanup());

            it('should reject with a error', () => {
                const errMsg = new RegExp('Could not retrieve code for: failing-asset-reader');
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });
    });
});
