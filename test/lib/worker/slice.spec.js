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

describe('Slice', () => {
    let jobConfig;
    const eventMocks = {
        'slice:success': jest.fn(),
        'slice:finalize': jest.fn(),
        'slice:failure': jest.fn(),
        'slice:retry': jest.fn(),
    };
    const readerFn = jest.fn();
    const opFn = jest.fn();

    function mockEvents(events) {
        Object.keys(eventMocks).forEach((name) => {
            const mock = eventMocks[name];
            events.on(name, mock);
        });
    }

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
        Object.values(eventMocks).forEach((mock) => {
            mock.mockRestore();
        });
        readerFn.mockRestore();
        opFn.mockRestore();
        exampleReaderMock.mockRestore();
        exampleOpMock.mockRestore();
        exampleReaderMock.mockResolvedValue(readerFn);
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

            beforeEach(async () => {
                _testContext = new TestContext('slice:analytics');

                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValue(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig, _testContext.stores);
                overrideLoggerOnWorker(slice, 'slice:no-analytics');

                const sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };

                await _testContext.addStateStore(slice.context);
                await slice.initialize(executionApi, sliceConfig);

                mockEvents(slice.events);

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

            it('should call the correct events', () => {
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalled();
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
                expect(eventMocks['slice:retry']).not.toHaveBeenCalled();
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
            let results;

            beforeEach(async () => {
                _testContext = new TestContext('slice:no-analytics');

                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValue(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                overrideLoggerOnWorker(slice, 'slice:no-analytics');

                const sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };

                await _testContext.addStateStore(slice.context);

                await slice.initialize(executionApi, sliceConfig, _testContext.stores);

                mockEvents(slice.events);

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

            it('should call the correct events', () => {
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalled();
                expect(eventMocks['slice:retry']).not.toHaveBeenCalled();
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
            });
        });

        describe('when the slice retries', () => {
            let slice;
            let _testContext;
            let results;
            let sliceConfig;

            beforeEach(async () => {
                jobConfig.job.max_retries = 3;
                _testContext = new TestContext('slice:retry');

                readerFn.mockRejectedValueOnce(new Error('Bad news bears'));
                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockResolvedValueOnce(times(10, () => 'hi'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                overrideLoggerOnWorker(slice, 'slice:retry');

                sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };

                await _testContext.addStateStore(slice.context);

                await slice.initialize(executionApi, sliceConfig, _testContext.stores);

                mockEvents(slice.events);

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

            it('should call the correct events', () => {
                expect(eventMocks['slice:retry']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:retry']).toHaveBeenCalledWith(sliceConfig);
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalledWith(sliceConfig);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledWith(sliceConfig);
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
            });
        });

        describe('when the slice fails', () => {
            let slice;
            let _testContext;
            let results;
            let err;
            let sliceConfig;

            beforeEach(async () => {
                jobConfig.job.max_retries = 5;
                _testContext = new TestContext('slice:failure');

                readerFn.mockResolvedValue(times(10, () => 'hello'));
                opFn.mockRejectedValue(new Error('Bad news bears'));

                const job = new Job(_testContext.context, jobConfig);
                const executionApi = await job.initialize();

                slice = new Slice(_testContext.config, jobConfig);
                overrideLoggerOnWorker(slice, 'slice:failure');

                sliceConfig = {
                    sliceId: 'some-slice-id',
                    request: {
                        example: 'slice-data'
                    }
                };

                await _testContext.addStateStore(slice.context);
                await slice.initialize(executionApi, sliceConfig, _testContext.stores);

                mockEvents(slice.events);

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
                expect(err.toString()).toContain('Slice failed processing: Error: Bad news bears');
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
                expect(eventMocks['slice:retry']).toHaveBeenCalledTimes(5);
                expect(eventMocks['slice:retry']).toHaveBeenCalledWith(sliceConfig);
                expect(eventMocks['slice:failure']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:failure']).toHaveBeenCalledWith(sliceConfig);
                expect(eventMocks['slice:success']).not.toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledWith(sliceConfig);
            });
        });
    });
});
