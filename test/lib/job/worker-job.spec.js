'use strict';

const fs = require('fs-extra');
const path = require('path');
const get = require('lodash/get');
const Job = require('../../../lib/job');
const TestContext = require('../../helpers/test-context');
const saveAsset = require('../../helpers/save-asset');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReaderMock = require(path.join(opsPath, 'example-reader')).newReader;
const exampleOpMock = require(path.join(opsPath, 'example-op')).newProcessor;
const exampleAssetDir = path.join(opsPath, 'example-asset');

describe('Worker Job', () => {
    describe('when constructing', () => {
        let job;
        let jobConfig;
        let _testContext;

        beforeEach(() => {
            _testContext = new TestContext('worker-job');
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
                ex_id: 'example-ex-id',
                job_id: 'example-job-id',
                slicer_port: 0
            };
            job = new Job(_testContext.context, jobConfig);
        });

        afterEach(() => _testContext.cleanup());

        it('should throw an error if reporters are specified', () => {
            _testContext.context.sysconfig.teraslice.reporter = true;
            expect(() => {
                new Job(_testContext.context, { hello: true }); // eslint-disable-line no-new
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
            let jobConfig;
            let _testContext;

            beforeEach(() => {
                _testContext = new TestContext('worker-job:fail');
                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: [],
                        operations: [
                            {
                                _op: null,
                            }
                        ]
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
            });

            afterEach(() => _testContext.cleanup());

            it('should reject with an error', () => {
                const errMsg = 'please verify that ops_directory in config and _op for each job operations are strings';
                return expect(job.initialize()).rejects.toThrow(errMsg);
            });
        });

        describe('when analytics is not enabled', () => {
            let executionApi;
            let job;
            let jobConfig;
            let _testContext;

            beforeEach(() => {
                _testContext = new TestContext('worker-job:no-analytics');
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
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
                exampleReaderMock.mockResolvedValue(jest.fn());
                exampleOpMock.mockResolvedValue(jest.fn());

                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeNil();
                }).catch((err) => {
                    console.error(err.stack); // eslint-disable-line no-console
                    expect(err).toBeNil();
                });
            });

            afterEach(() => _testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeNil();
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
                expect(reporter).toBeNil();
                expect(slicer).toBeNil();
            });

            it('should load the ops', () => {
                expect(exampleReaderMock).toHaveBeenCalledTimes(1);
                expect(exampleReaderMock).toHaveBeenCalledWith(_testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, jobConfig.job);
                expect(exampleOpMock).toHaveBeenCalledTimes(1);
                expect(exampleOpMock).toHaveBeenCalledWith(_testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, jobConfig.job);
            });
        });

        describe('when analytics is enabled', () => {
            let executionApi;
            let job;
            let jobConfig;
            let _testContext;

            beforeEach(() => {
                _testContext = new TestContext('worker-job:analytics');
                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: [],
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
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
                exampleReaderMock.mockResolvedValue(jest.fn());
                exampleOpMock.mockResolvedValue(jest.fn());

                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeNil();
                }).catch((err) => {
                    console.error(err.stack); // eslint-disable-line no-console
                    expect(err).toBeNil();
                });
            });

            afterEach(() => _testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeNil();
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
                expect(reporter).toBeNil();
                expect(slicer).toBeNil();
            });

            it('should load the ops', () => {
                expect(exampleReaderMock).toHaveBeenCalledTimes(1);
                expect(exampleReaderMock).toHaveBeenCalledWith(_testContext.context, {
                    _op: path.join(opsPath, 'example-reader'),
                    exampleProp: 321
                }, jobConfig.job);
                expect(exampleOpMock).toHaveBeenCalledTimes(1);
                expect(exampleOpMock).toHaveBeenCalledWith(_testContext.context, {
                    _op: path.join(opsPath, 'example-op'),
                    exampleProp: 123
                }, jobConfig.job);
            });
        });

        describe('when using assets', () => {
            let executionApi;
            let job;
            let jobConfig;
            let _testContext;

            beforeAll(async () => {
                _testContext = new TestContext('worker-job:assets');
                await saveAsset(_testContext.context, exampleAssetDir);
                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: ['example-asset'],
                        operations: [
                            {
                                _op: 'example-asset-reader',
                            },
                            {
                                _op: 'example-asset-op',
                            }
                        ]
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeNil();
                }).catch((err) => {
                    console.error(err.stack); // eslint-disable-line no-console
                    expect(err).toBeNil();
                });
            });

            afterAll(() => _testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeNil();
                const {
                    queue,
                    config,
                    reader,
                    reporter,
                    slicer
                } = executionApi;
                expect(queue).toBeArrayOfSize(2);
                expect(reader).toBeFunction();
                expect(queue[1]).toBeFunction();
                expect(config).toEqual(jobConfig.job);
                expect(reporter).toBeNil();
                expect(slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await executionApi.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await executionApi.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets that have not been downloaded', () => {
            let executionApi;
            let job;
            let jobConfig;
            let _testContext;

            beforeAll(async () => {
                _testContext = new TestContext('worker-job:assets-download');

                const assetId = await saveAsset(_testContext.context, exampleAssetDir);
                await fs.remove(path.join(_testContext.assetDir, assetId));

                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: ['example-asset'],
                        operations: [
                            {
                                _op: 'example-asset-reader',
                            },
                            {
                                _op: 'example-asset-op',
                            }
                        ]
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
                return job.initialize().then((_executionApi) => {
                    executionApi = _executionApi;
                    expect(_executionApi).not.toBeNil();
                }).catch((err) => {
                    console.error(err.stack); // eslint-disable-line no-console
                    expect(err).toBeNil();
                });
            });

            afterAll(() => _testContext.cleanup());

            it('should resolve an execution api', () => {
                expect(executionApi).not.toBeNil();
                const {
                    queue,
                    config,
                    reader,
                    reporter,
                    slicer
                } = executionApi;
                expect(queue).toBeArrayOfSize(2);
                expect(reader).toBeFunction();
                expect(queue[1]).toBeFunction();
                expect(config).toEqual(jobConfig.job);
                expect(reporter).toBeNil();
                expect(slicer).toBeNil();
            });

            it('should load the ops', async () => {
                const readerResults = await executionApi.reader();
                expect(readerResults).toBeArrayOfSize(100);
                const opResults = await executionApi.queue[1](readerResults);
                expect(opResults).toBeArrayOfSize(100);
            });
        });

        describe('when using assets and they do not exist', () => {
            let job;
            let jobConfig;
            let _testContext;

            beforeAll(async () => {
                _testContext = new TestContext('worker-job:assets-fail');

                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: ['missing-assets'],
                        operations: [
                            {
                                _op: 'missing-assets-reader',
                            },
                            {
                                _op: 'missing-assets-op',
                            }
                        ]
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
            });

            afterAll(() => _testContext.cleanup());

            it('should reject with a error', () => {
                const errMsg = 'asset: missing-assets was not found';
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });

        describe('when using assets and the fail on require', () => {
            let job;
            let jobConfig;
            let _testContext;

            beforeAll(async () => {
                _testContext = new TestContext('worker-job:failing-asset');
                await saveAsset(_testContext.context, path.join(opsPath, 'failing-asset'));

                jobConfig = {
                    type: 'worker',
                    job: {
                        assets: ['failing-asset'],
                        operations: [
                            {
                                _op: 'failing-asset-reader',
                            }
                        ]
                    },
                    ex_id: 'example-ex-id',
                    job_id: 'example-job-id',
                    slicer_port: 0,
                };
                job = new Job(_testContext.context, jobConfig);
            });

            afterAll(() => _testContext.cleanup());

            it('should reject with a error', () => {
                const errMsg = new RegExp('Could not retrieve code for: failing-asset-reader');
                return expect(job.initialize()).rejects.toThrowError(errMsg);
            });
        });
    });
});
