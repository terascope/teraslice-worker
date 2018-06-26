'use strict';

const times = require('lodash/times');
const path = require('path');
const Slice = require('../../../lib/worker/slice');
const Job = require('../../../lib/job');
const TestContext = require('../../helpers/test-context');
const { overrideLoggerOnWorker } = require('../../helpers/override-logger');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReaderMock = require(path.join(opsPath, 'example-reader')).newReader;
const exampleOpMock = require(path.join(opsPath, 'example-op')).newProcessor;
const readerFn = jest.fn();
const opFn = jest.fn();


describe('Slice', () => {
    let jobConfig;

    beforeEach(() => {
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
            jobId: 'example-job-id',
            slicerPort: 0,
        };

        exampleReaderMock.mockRestore();
        readerFn.mockRestore();
        exampleReaderMock.mockResolvedValue(readerFn);

        exampleOpMock.mockRestore();
        opFn.mockRestore();
        exampleOpMock.mockResolvedValue(opFn);
    });

    describe('with analytics', () => {
        beforeEach(() => {
            jobConfig.job.analytics = true;
        });

        describe('when the slice succeeds', () => {
            let slice;
            let _testContext;
            let results;
            let successFn;

            beforeEach(async () => {
                _testContext = new TestContext('slice:analytics');
                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValue(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                const sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };
                await slice.initialize(executionApi, sliceConfig);
                overrideLoggerOnWorker(slice, 'slice:no-analytics');

                successFn = jest.fn();
                slice.events.on('slice:success', successFn);

                results = await slice.start();
            });

            afterEach(() => _testContext.cleanup());

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(readerFn).toHaveBeenCalledTimes(1);
                expect(readerFn).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(opFn).toHaveBeenCalledTimes(1);
                expect(opFn).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should modify change the specData', () => {
                expect(slice.specData).toBeObject();
                expect(slice.specData.memory).toBeArrayOfSize(2);
                expect(slice.specData.size).toBeArrayOfSize(2);
                expect(slice.specData.time).toBeArrayOfSize(2);
            });

            it('should call slice success', () => {
                expect(successFn).toHaveBeenCalled();
            });
        });
    });

    describe('without analytics', () => {
        beforeEach(() => {
            jobConfig.job.analytics = false;
        });

        describe('when the slice succeeds', () => {
            let slice;
            let _testContext;
            let successFn;
            let results;

            beforeEach(async () => {
                _testContext = new TestContext('slice:no-analytics');

                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValue(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                const sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };
                await slice.initialize(executionApi, sliceConfig);
                overrideLoggerOnWorker(slice, 'slice:no-analytics');
                successFn = jest.fn();
                slice.events.on('slice:success', successFn);


                results = await slice.start();
            });

            afterEach(() => _testContext.cleanup());

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(readerFn).toHaveBeenCalledTimes(1);
                expect(readerFn).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(opFn).toHaveBeenCalledTimes(1);
                expect(opFn).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('specData');
            });

            it('should call slice success', () => {
                expect(successFn).toHaveBeenCalled();
            });
        });

        describe('when the slice retries', () => {
            let slice;
            let _testContext;
            let results;
            let successFn;
            let retryFn;
            let sliceConfig;

            beforeEach(() => {
                retryFn = jest.fn();
                successFn = jest.fn();
                jobConfig.job.max_retries = 3;
            });

            beforeEach(async () => {
                _testContext = new TestContext('slice:retry');
                readerFn.mockRejectedValueOnce(new Error('Bad news bears'));
                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValueOnce(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };
                await slice.initialize(executionApi, sliceConfig);
                overrideLoggerOnWorker(slice, 'slice:retry');

                slice.events.on('slice:retry', retryFn);
                slice.events.on('slice:success', successFn);

                results = await slice.start();
            });

            afterEach(() => _testContext.cleanup());

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(readerFn).toHaveBeenCalledTimes(2);
                expect(readerFn).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(opFn).toHaveBeenCalledTimes(1);
                expect(opFn).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('specData');
            });

            it('should emit "slice:retry"', () => {
                expect(retryFn).toHaveBeenCalledTimes(1);
                expect(retryFn).toHaveBeenCalledWith(sliceConfig);
            });

            it('should emit "slice:success"', () => {
                expect(successFn).toHaveBeenCalledTimes(1);
                expect(successFn).toHaveBeenCalledWith(sliceConfig);
            });
        });

        describe('when the slice fails', () => {
            let slice;
            let _testContext;
            let results;
            let retryFn;
            let failureFn;
            let successFn;
            let err;
            let sliceConfig;

            beforeEach(() => {
                retryFn = jest.fn();
                failureFn = jest.fn();
                successFn = jest.fn();
                jobConfig.job.max_retries = 5;
            });

            beforeEach(async () => {
                _testContext = new TestContext('slice:failure');
                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockRejectedValue(new Error('Bad news bears'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };
                await slice.initialize(executionApi, sliceConfig);
                overrideLoggerOnWorker(slice, 'slice:failure');

                slice.events.on('slice:retry', retryFn);
                slice.events.on('slice:failure', failureFn);
                slice.events.on('slice:success', successFn);

                try {
                    results = await slice.start();
                } catch (_err) {
                    err = _err;
                }
            });

            afterEach(() => _testContext.cleanup());

            it('should not have any results', () => {
                expect(results).not.toBeDefined();
            });

            it('should have reject with the error', () => {
                expect(err).toThrowError('Slice failed processing: Error: Bad news bears');
            });

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(readerFn).toHaveBeenCalledTimes(5);
                expect(readerFn).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(opFn).toHaveBeenCalledTimes(5);
                expect(opFn).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);
            });

            it('should emit the events', () => {
                expect(retryFn).toHaveBeenCalledTimes(5);
                expect(retryFn).toHaveBeenCalledWith(sliceConfig);
                expect(failureFn).toHaveBeenCalledTimes(1);
                expect(failureFn).toHaveBeenCalledWith(sliceConfig);
                expect(successFn).not.toHaveBeenCalled();
            });
        });
    });
});
