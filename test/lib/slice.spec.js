'use strict';

const times = require('lodash/times');
const Slice = require('../../lib/slice');
const Job = require('../../lib/job');
const {
    newSliceConfig,
    overrideLogger,
    TestContext,
    newJobConfig
} = require('../helpers');

const exampleReader = require('../fixtures/ops/example-reader');
const exampleOp = require('../fixtures/ops/example-op');

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

    async function setupSlice(options) {
        const testContext = new TestContext('slice:analytics');

        const jobConfig = newJobConfig(options);
        const sliceConfig = newSliceConfig();

        const job = new Job(testContext.context, jobConfig);
        await job.initialize();

        const slice = new Slice(testContext.context, jobConfig);
        overrideLogger(slice, 'slice');

        await testContext.addStateStore(slice.context);
        await testContext.addAnalyticsStore(slice.context);
        const { stores } = testContext;
        await stores.stateStore.createState(jobConfig.ex_id, sliceConfig, 'start');

        await slice.initialize(job, sliceConfig, stores);

        cleanupTasks.push(() => testContext.cleanup());

        return slice;
    }

    async function cleanup() {
        while (cleanupTasks.length > 0) {
            const task = cleanupTasks.shift();
            await task(); // eslint-disable-line no-await-in-loop
        }
    }

    beforeAll(async () => {
        await cleanup();
    });

    afterAll(async () => {
        await cleanup();
    });

    describe('with analytics', () => {
        describe('when the slice succeeds', () => {
            let results;
            let slice;
            let mocks;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));

                slice = await setupSlice({ analytics: true });
                mockEvents(slice.events, mocks.events);

                results = await slice.run();
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

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:completed`;
                return expect(slice.stateStore.count(query)).resolves.toEqual(1);
            });
        });
    });

    describe('without analytics', () => {
        describe('when the slice succeeds', () => {
            let slice;
            let results;
            let mocks;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));

                slice = await setupSlice();
                mockEvents(slice.events, mocks.events);

                results = await slice.run();
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

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:completed`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });

        describe('when the slice retries', () => {
            let slice;
            let results;
            let mocks;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockRejectedValueOnce(new Error('Bad news bears'));
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValueOnce(times(10, () => 'hi'));

                slice = await setupSlice({ maxRetries: 3 });
                mockEvents(slice.events, mocks.events);

                results = await slice.run();
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

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:completed`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });

        describe('when the slice fails', () => {
            let slice;
            let results;
            let err;
            let mocks;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockRejectedValue(new Error('Bad news bears'));

                slice = await setupSlice({ maxRetries: 5 });
                mockEvents(slice.events, mocks.events);

                try {
                    results = await slice.run();
                } catch (_err) {
                    err = _err;
                }
            });

            it('should not have any results', () => {
                expect(results).not.toBeDefined();
            });

            it('should have reject with the error', () => {
                expect(err.toString()).toStartWith('Error: Slice failed processing, caused by Error: Bad news bears');
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

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:error`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });
    });

    describe('when given a completed slice', () => {
        let slice;
        let mocks;

        beforeAll(async () => {
            mocks = makeMocks();
            mocks.reader.mockResolvedValue(times(10, () => 'hello'));
            mocks.op.mockResolvedValue(times(10, () => 'hi'));

            slice = await setupSlice();
            await slice._markCompleted();
            mockEvents(slice.events, mocks.events);
        });

        it('should throw an error when calling run', () => {
            const errMsg = `Slice ${slice.slice.slice_id} has already been processed`;
            return expect(slice.run()).rejects.toThrowError(errMsg);
        });
    });

    describe('when logging the analytics state', () => {
        describe('when given invalid state', () => {
            let mocks;
            let slice;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));
                slice = await setupSlice({ analytics: true });
            });

            it('should throw an error if given invalid state', async () => {
                slice.analyticsData = { should: 'break' };
                return expect(slice._logAnalytics()).rejects.toThrowError(/Failure to update analytics/);
            });
        });

        describe('when the slice is a string', () => {
            let mocks;
            let slice;

            beforeAll(async () => {
                mocks = makeMocks();
                mocks.reader.mockResolvedValue(times(10, () => 'hello'));
                mocks.op.mockResolvedValue(times(10, () => 'hi'));
                slice = await setupSlice({ analytics: true });
            });

            it('should handle the case when the slice is a string', async () => {
                slice.slice = 'hello-there';
                await slice._logAnalytics();
            });
        });
    });

    describe('when marking the slice as complete', () => {
        it('should throw an error if given invalid state', async () => {
            const slice = await setupSlice();
            slice.slice = { should: 'break' };
            return expect(slice._markCompleted()).rejects.toThrowError(/Failure to update success state/);
        });
    });

    describe('when marking the slice as failed', () => {
        it('should throw an error if given invalid state', async () => {
            const slice = await setupSlice();
            slice.slice = { should: 'break' };
            await expect(slice._markFailed(new Error('some error'))).rejects.toThrowError(/Failure to update failed state/);
            await expect(slice._markFailed()).rejects.toThrowError(/Failure to update failed state/);
        });
    });
});
