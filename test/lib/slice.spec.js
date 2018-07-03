'use strict';

const times = require('lodash/times');
const Slice = require('../../lib/slice');
const Job = require('../../lib/job');
const {
    overrideLogger,
    TestContext,
} = require('../helpers');

jest.setTimeout(10000);

describe('Slice', () => {
    let slice;
    let testContext;
    let eventMocks;

    async function setupSlice(options) {
        testContext = new TestContext('slice:analytics', options);

        const job = new Job(testContext.context, testContext.jobConfig);
        await job.initialize();

        slice = new Slice(testContext.context, testContext.jobConfig);
        overrideLogger(slice, 'slice');

        await testContext.addStateStore();
        await testContext.addAnalyticsStore();

        await testContext.newSlice();

        await slice.initialize(job, testContext.sliceConfig, testContext.stores);

        eventMocks = {
            'slice:success': jest.fn(),
            'slice:finalize': jest.fn(),
            'slice:failure': jest.fn(),
            'slice:retry': jest.fn(),
        };

        Object.keys(eventMocks).forEach((name) => {
            const mock = eventMocks[name];
            slice.events.on(name, mock);
        });
    }

    afterEach(() => testContext.cleanup());

    describe('with analytics', () => {
        describe('when the slice succeeds', () => {
            let results;

            beforeEach(async () => {
                await setupSlice({ analytics: true });

                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockResolvedValue(times(10, () => 'hi'));

                results = await slice.run();
            });

            it('should call all of the operations', () => {
                const { reader, op } = testContext;
                const sliceRequest = { example: 'slice-data' };

                expect(reader).toHaveBeenCalledTimes(1);
                expect(reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(op).toHaveBeenCalledTimes(1);
                expect(op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should modify change the analyticsData', () => {
                expect(slice.analyticsData).toBeObject();
                expect(slice.analyticsData.memory).toBeArrayOfSize(2);
                expect(slice.analyticsData.size).toBeArrayOfSize(2);
                expect(slice.analyticsData.time).toBeArrayOfSize(2);
            });

            it('should call the correct events', () => {
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalled();
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
                expect(eventMocks['slice:retry']).not.toHaveBeenCalled();
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
            let results;

            beforeEach(async () => {
                await setupSlice({ analytics: false });

                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockResolvedValue(times(10, () => 'hi'));

                results = await slice.run();
            });

            it('should call all of the operations', () => {
                const { reader, op } = testContext;

                const sliceRequest = { example: 'slice-data' };
                expect(reader).toHaveBeenCalledTimes(1);
                expect(reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(op).toHaveBeenCalledTimes(1);
                expect(op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('analyticsData');
            });

            it('should call the correct events', () => {
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalled();
                expect(eventMocks['slice:retry']).not.toHaveBeenCalled();
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
            });

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:completed`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });

        describe('when the slice retries', () => {
            let results;

            beforeEach(async () => {
                await setupSlice({ maxRetries: 3, analytics: false });

                testContext.reader.mockRejectedValueOnce(new Error('Bad news bears'));
                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockResolvedValue(times(10, () => 'hi'));

                results = await slice.run();
            });

            it('should call all of the operations', () => {
                const { reader, op } = testContext;
                const sliceRequest = { example: 'slice-data' };
                expect(reader).toHaveBeenCalledTimes(2);
                expect(reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(op).toHaveBeenCalledTimes(1);
                expect(op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

                expect(results).toEqual(times(10, () => 'hi'));
            });

            it('should have have the spec data', () => {
                expect(slice).not.toHaveProperty('analyticsData');
            });

            it('should call the correct events', () => {
                expect(eventMocks['slice:retry']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:retry']).toHaveBeenCalledWith(slice.slice);
                expect(eventMocks['slice:success']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:success']).toHaveBeenCalledWith(slice.slice);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledWith(slice.slice);
                expect(eventMocks['slice:failure']).not.toHaveBeenCalled();
            });

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:completed`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });

        describe('when the slice fails', () => {
            let results;
            let err;

            beforeEach(async () => {
                await setupSlice({ maxRetries: 5, analytics: false });

                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockRejectedValue(new Error('Bad news bears'));

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
                const { reader, op } = testContext;

                const sliceRequest = { example: 'slice-data' };

                expect(reader).toHaveBeenCalledTimes(5);
                expect(reader).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

                const readerResults = times(10, () => 'hello');
                expect(op).toHaveBeenCalledTimes(5);
                expect(op).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);
            });

            it('should emit the events', () => {
                expect(eventMocks['slice:retry']).toHaveBeenCalledTimes(5);
                expect(eventMocks['slice:retry']).toHaveBeenCalledWith(slice.slice);
                expect(eventMocks['slice:failure']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:failure']).toHaveBeenCalledWith(slice.slice);
                expect(eventMocks['slice:success']).not.toHaveBeenCalled();
                expect(eventMocks['slice:finalize']).toHaveBeenCalledTimes(1);
                expect(eventMocks['slice:finalize']).toHaveBeenCalledWith(slice.slice);
            });

            it('should have the correct state storage', () => {
                const { ex_id: exId } = slice.jobConfig;
                const query = `ex_id:${exId} AND state:error`;
                return expect(slice.stateStore.count(query, 0)).resolves.toEqual(1);
            });
        });
    });

    describe('when given a completed slice', () => {
        beforeEach(async () => {
            await setupSlice();

            testContext.reader.mockResolvedValue(times(10, () => 'hello'));
            testContext.op.mockResolvedValue(times(10, () => 'hi'));

            await slice._markCompleted();
        });

        it('should throw an error when calling run', () => {
            const errMsg = `Slice ${slice.slice.slice_id} has already been processed`;
            return expect(slice.run()).rejects.toThrowError(errMsg);
        });
    });

    describe('when logging the analytics state', () => {
        describe('when given invalid state', () => {
            beforeEach(async () => {
                await setupSlice({ analytics: true });
                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockResolvedValue(times(10, () => 'hi'));
            });

            it('should throw an error if given invalid state', async () => {
                slice.analyticsData = { should: 'break' };
                return expect(slice._logAnalytics()).rejects.toThrowError(/Failure to update analytics/);
            });
        });

        describe('when the slice is a string', () => {
            beforeEach(async () => {
                await setupSlice({ analytics: true });

                testContext.reader.mockResolvedValue(times(10, () => 'hello'));
                testContext.op.mockResolvedValue(times(10, () => 'hi'));
            });

            it('should handle the case when the slice is a string', async () => {
                slice.slice = 'hello-there';
                await slice._logAnalytics();
            });
        });
    });

    describe('when marking the slice as complete', () => {
        it('should throw an error if given invalid state', async () => {
            await setupSlice();

            slice.slice = { should: 'break' };

            return expect(slice._markCompleted()).rejects.toThrowError(/Failure to update success state/);
        });
    });

    describe('when marking the slice as failed', () => {
        it('should throw an error if given invalid state', async () => {
            await setupSlice();

            slice.slice = { should: 'break' };

            await expect(slice._markFailed(new Error('some error'))).rejects.toThrowError(/Failure to update failed state/);
            await expect(slice._markFailed()).rejects.toThrowError(/Failure to update failed state/);
        });
    });
});
