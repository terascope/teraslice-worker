'use strict';

const path = require('path');
const get = require('lodash/get');
const Job = require('../../lib/job');
const { TestContext, opsPath } = require('../helpers');

const exampleAssetDir = path.join(opsPath, 'example-asset');

describe('Job', () => {
    describe('when constructed', () => {
        let job;
        let testContext;

        beforeAll(() => {
            testContext = new TestContext('worker-job', {
                assignment: 'worker',
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

        afterAll(() => testContext.cleanup());

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

    describe('when using assignment "worker"', () => {
        describe('when op name is not a string', () => {
            let job;
            let testContext;
            beforeAll(() => {
                testContext = new TestContext('worker-job:fail', {
                    assignment: 'worker',
                    operations: [
                        {
                            _op: null,
                        }
                    ]
                });
                job = new Job(testContext.context, testContext.jobConfig);
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should reject with an error', () => {
                const errMsg = 'please verify that ops_directory in config and _op for each job operations are strings';
                return expect(job.initialize()).rejects.toThrow(errMsg);
            });
        });

        describe('when analytics is not enabled', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job', {
                    assignment: 'worker',
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

                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(2);
                expect(executionContext.queue[0]).toBeFunction();
                expect(executionContext.queue[1]).toBeFunction();
                expect(executionContext.reader).toEqual(executionContext.queue[0]);
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toBeNil();
            });

            it('should load the ops', () => {
                const { newReader } = testContext.exampleReader;
                const { newProcessor } = testContext.exampleOp;
                expect(newReader).toHaveBeenCalledTimes(1);
                expect(newReader).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, testContext.jobConfig.job);
                expect(newProcessor).toHaveBeenCalledTimes(1);
                expect(newProcessor).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, testContext.jobConfig.job);
            });
        });

        describe('when analytics is enabled', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:analytics', {
                    assignment: 'worker',
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
                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(2);
                expect(executionContext.queue[0]).toBeFunction();
                expect(executionContext.queue[1]).toBeFunction();
                expect(executionContext.reader).toEqual(executionContext.queue[0]);
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toBeNil();
            });

            it('should load the ops', () => {
                const { newReader } = testContext.exampleReader;
                const { newProcessor } = testContext.exampleOp;
                expect(newReader).toHaveBeenCalledTimes(1);
                expect(newReader).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, testContext.jobConfig.job);
                expect(newProcessor).toHaveBeenCalledTimes(1);
                expect(newProcessor).toHaveBeenCalledWith(testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, testContext.jobConfig.job);
            });
        });

        describe('when using assets', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets', {
                    assignment: 'worker',
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

                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(2);
                expect(executionContext.reader).toBeFunction();
                expect(executionContext.queue[1]).toBeFunction();
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await executionContext.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await executionContext.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets that have not been downloaded', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets-download', {
                    assignment: 'worker',
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
                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(2);
                expect(executionContext.reader).toBeFunction();
                expect(executionContext.queue[1]).toBeFunction();
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await executionContext.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await executionContext.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets and they do not exist', () => {
            let job;
            let testContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets-fail', {
                    assignment: 'worker',
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

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should reject with a error', () => {
                const errMsg = 'asset: missing-assets was not found';
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });

        describe('when using assets and the fail on require', () => {
            let job;
            let testContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:failing-asset', {
                    assignment: 'worker',
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

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should reject with a error', () => {
                const errMsg = new RegExp('Could not retrieve code for: failing-asset-reader');
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });
    });

    describe('when using assignment "execution_controller"', () => {
        describe('when op name is not a string', () => {
            let job;
            let testContext;
            beforeAll(() => {
                testContext = new TestContext('worker-job:fail', {
                    assignment: 'execution_controller',
                    operations: [
                        {
                            _op: null,
                        }
                    ]
                });
                job = new Job(testContext.context, testContext.jobConfig);
            });

            afterAll(async () => {
                await job.shutdown();
                await testContext.cleanup();
            });

            it('should reject with an error', () => {
                const errMsg = 'please verify that ops_directory in config and _op for each job operations are strings';
                return expect(job.initialize()).rejects.toThrow(errMsg);
            });
        });

        describe('when using a valid job configuration', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job', {
                    assignment: 'execution_controller',
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

                testContext.attachCleanup(() => job.shutdown());

                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(0);
                expect(executionContext.reader).toBeNil();
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toHaveProperty('newSlicer');
            });

            it('should not call newSlicer', () => {
                expect(testContext.exampleReader.newSlicer).not.toHaveBeenCalled();
            });
        });

        describe('when using assets', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets', {
                    assignment: 'execution_controller',
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

                testContext.attachCleanup(() => job.shutdown());

                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(0);
                expect(executionContext.reader).toBeNil();
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toHaveProperty('newSlicer');
            });

            it('should be able to run the slicer', async () => {
                const slicer = await executionContext.slicer.newSlicer();
                const results = await slicer();
                expect(results).toBeArrayOfSize(100);
            });
        });

        describe('when using assets that have not been downloaded', () => {
            let job;
            let testContext;
            let executionContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets-download', {
                    assignment: 'execution_controller',
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

                testContext.attachCleanup(() => job.shutdown());

                executionContext = await job.initialize();
            });

            afterAll(async () => {
                await testContext.cleanup();
            });

            it('should resolve an execution api', () => {
                expect(executionContext.queue).toBeArrayOfSize(0);
                expect(executionContext.reader).toBeNil();
                expect(executionContext.config).toEqual(testContext.jobConfig.job);
                expect(executionContext.reporter).toBeNil();
                expect(executionContext.slicer).toHaveProperty('newSlicer');
            });

            it('should be able to run the slicer', async () => {
                const slicer = await executionContext.slicer.newSlicer();
                const results = await slicer();
                expect(results).toBeArrayOfSize(100);
            });
        });

        describe('when using assets and they do not exist', () => {
            let job;
            let testContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:assets-fail', {
                    assignment: 'execution_controller',
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

                testContext.attachCleanup(() => job.shutdown());
            });

            afterAll(async () => {
                await testContext.cleanup();
            });

            it('should reject with a error', () => {
                const errMsg = 'asset: missing-assets was not found';
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });

        describe('when using assets and the fail on require', () => {
            let job;
            let testContext;

            beforeAll(async () => {
                testContext = new TestContext('worker-job:failing-asset', {
                    assignment: 'execution_controller',
                    assets: ['failing-asset'],
                    operations: [
                        {
                            _op: 'failing-asset-reader',
                        }
                    ]
                });
                await testContext.saveAsset(path.join(opsPath, 'failing-asset'));
                job = new Job(testContext.context, testContext.jobConfig);

                testContext.attachCleanup(() => job.shutdown());
            });

            afterAll(async () => {
                await testContext.cleanup();
            });

            it('should reject with a error', () => {
                const errMsg = new RegExp('Could not retrieve code for: failing-asset-reader');
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });
    });
});
