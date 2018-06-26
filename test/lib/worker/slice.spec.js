'use strict';


const shortid = require('shortid');
const times = require('lodash/times');
const random = require('lodash/random');
const path = require('path');
const Slice = require('../../../lib/worker/slice');
const Job = require('../../../lib/job');
const TestContext = require('../../helpers/test-context');
const { overrideLoggerOnWorker } = require('../../helpers/override-logger');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReader = require('../../fixtures/ops/example-reader');
const exampleOp = require('../../fixtures/ops/example-op');

const newId = prefix => `${prefix}-${shortid.generate()}`.toLowerCase();

function makeMocks() {
    const events = {
        'slice:success': jest.fn(),
        'slice:finalize': jest.fn(),
        'slice:failure': jest.fn(),
        'slice:retry': jest.fn(),
    };

    const reader = jest.fn();
    const op = jest.fn();
    exampleReader.newReader = jest.fn().mockResolvedValue(reader);
    exampleOp.newProcessor = jest.fn().mockResolvedValue(op);
    return {
        events,
        reader,
        op,
    };
}

describe('Slice', () => {
    const cleanupTasks = [];

    function mockEvents(events, eventMocks) {
        Object.keys(eventMocks).forEach((name) => {
            const mock = eventMocks[name];
            events.on(name, mock);
        });
    }


    async function setupSlice({ analytics = false, maxRetries = 1 } = {}) {
        const jobConfig = {
            type: 'worker',
            job: {
                assets: [],
                analytics,
                max_retries: maxRetries,
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
            exId: newId('ex-id'),
            jobId: newId('job-id'),
            slicerPort: 0,
        };
        const testContext = new TestContext('slice:analytics');
        const job = new Job(testContext.context, jobConfig);
        const executionApi = await job.initialize();

        const slice = new Slice(testContext.config, jobConfig, testContext.stores);
        overrideLoggerOnWorker(slice, 'slice');

        const sliceConfig = {
            slice_id: newId('slice-id'),
            slicer_id: newId('slicer-id'),
            order: random(0, 1000),
            request: {
                example: 'slice-data'
            },
            _created: new Date().toISOString()
        };

        await testContext.addStateStore(slice.context);
        const { stateStore } = testContext.stores;
        await stateStore.createState(jobConfig.exId, sliceConfig, 'start');

        await slice.initialize(executionApi, sliceConfig, { stateStore });

        cleanupTasks.push(() => testContext.cleanup());

        return slice;
    }

    async function cleanup() {
        while (cleanupTasks.length > 0) {
            const task = cleanupTasks.shift();
            await task(); // eslint-disable-line no-await-in-loop
        }
    }

    beforeEach(async () => {
        await cleanup();
    });

    afterEach(async () => {
        await cleanup();
    });

    describe('with analytics', () => {
        describe('when the slice succeeds', () => {
            let results;
            let slice;
            let mocks;

            beforeEach(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));

                slice = await setupSlice({ analytics: true });
                mockEvents(slice.events, mocks.events);

                results = await slice.start();
            });

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(mocks.reader).toHaveBeenCalledTimes(1);
                expect(mocks.reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(mocks.op).toHaveBeenCalledTimes(1);
                expect(mocks.op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should modify change the analyticsData', () => {
                expect(slice.analyticsData).toBeObject();
                expect(slice.analyticsData.memory).toBeArrayOfSize(2);
                expect(slice.analyticsData.size).toBeArrayOfSize(2);
                expect(slice.analyticsData.time).toBeArrayOfSize(2);
            });

            it('should call the correct events', () => {
                expect(mocks.events['slice:success']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:success']).toHaveBeenCalled();
                expect(mocks.events['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:finalize']).toHaveBeenCalled();
                expect(mocks.events['slice:failure']).not.toHaveBeenCalled();
                expect(mocks.events['slice:retry']).not.toHaveBeenCalled();
            });
        });
    });

    describe('without analytics', () => {
        describe('when the slice succeeds', () => {
            let slice;
            let results;
            let mocks;

            beforeEach(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));

                slice = await setupSlice();
                mockEvents(slice.events, mocks.events);

                results = await slice.start();
            });

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(mocks.reader).toHaveBeenCalledTimes(1);
                expect(mocks.reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(mocks.op).toHaveBeenCalledTimes(1);
                expect(mocks.op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('analyticsData');
            });

            it('should call the correct events', () => {
                expect(mocks.events['slice:success']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:success']).toHaveBeenCalled();
                expect(mocks.events['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:finalize']).toHaveBeenCalled();
                expect(mocks.events['slice:retry']).not.toHaveBeenCalled();
                expect(mocks.events['slice:failure']).not.toHaveBeenCalled();
            });
        });

        describe('when the slice retries', () => {
            let slice;
            let results;
            let mocks;

            beforeEach(async () => {
                mocks = makeMocks();
                mocks.reader.mockRejectedValueOnce(new Error('Bad news bears'));
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValueOnce(times(10, () => 'hi'));

                slice = await setupSlice({ maxRetries: 3 });
                mockEvents(slice.events, mocks.events);

                results = await slice.start();
            });

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(mocks.reader).toHaveBeenCalledTimes(2);
                expect(mocks.reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(mocks.op).toHaveBeenCalledTimes(1);
                expect(mocks.op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('analyticsData');
            });

            it('should call the correct events', () => {
                expect(mocks.events['slice:retry']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:retry']).toHaveBeenCalledWith(slice.slice);
                expect(mocks.events['slice:success']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:success']).toHaveBeenCalledWith(slice.slice);
                expect(mocks.events['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:finalize']).toHaveBeenCalledWith(slice.slice);
                expect(mocks.events['slice:failure']).not.toHaveBeenCalled();
            });
        });

        describe('when the slice fails', () => {
            let slice;
            let results;
            let err;
            let mocks;

            beforeEach(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockRejectedValue(new Error('Bad news bears'));

                slice = await setupSlice({ maxRetries: 5 });
                mockEvents(slice.events, mocks.events);

                try {
                    results = await slice.start();
                } catch (_err) {
                    err = _err;
                }
            });

            it('should not have any results', () => {
                expect(results).not.toBeDefined();
            });

            it('should have reject with the error', () => {
                expect(err.toString()).toContain('Slice failed processing: Error: Bad news bears');
            });

            it('should call all of the operations', () => {
                const sliceRequest = { example: 'slice-data' };
                expect(mocks.reader).toHaveBeenCalledTimes(5);
                expect(mocks.reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(mocks.op).toHaveBeenCalledTimes(5);
                expect(mocks.op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);
            });

            it('should emit the events', () => {
                expect(mocks.events['slice:retry']).toHaveBeenCalledTimes(5);
                expect(mocks.events['slice:retry']).toHaveBeenCalledWith(slice.slice);
                expect(mocks.events['slice:failure']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:failure']).toHaveBeenCalledWith(slice.slice);
                expect(mocks.events['slice:success']).not.toHaveBeenCalled();
                expect(mocks.events['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(mocks.events['slice:finalize']).toHaveBeenCalledWith(slice.slice);
            });
        });
    });
});
