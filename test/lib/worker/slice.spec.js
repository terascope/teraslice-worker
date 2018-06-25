'use strict';

const times = require('lodash/times');
const path = require('path');
const Slice = require('../../../lib/worker/slice');
const Job = require('../../../lib/job');
const TestContext = require('../../helpers/test-context');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReaderMock = require(path.join(opsPath, 'example-reader')).newReader;
const exampleOpMock = require(path.join(opsPath, 'example-op')).newProcessor;

describe('Slice', () => {
    describe('with analytics', () => {
        let slice;
        let _testContext;
        let readerFn;
        let opFn;
        let results;

        beforeEach(async () => {
            _testContext = new TestContext('slice');
            const jobConfig = {
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
                exId: 'example-ex-id',
                jobId: 'example-job-id',
                slicerPort: 0,
            };
            readerFn = jest.fn().mockResolvedValue(times(10, () => 'hello'));
            opFn = jest.fn().mockResolvedValue(times(10, () => 'hi'));
            exampleReaderMock.mockResolvedValue(readerFn);
            exampleOpMock.mockResolvedValue(opFn);

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
            results = await slice.start();
        });

        afterEach(() => _testContext.cleanup());

        it('should call all of the operations', () => {
            const sliceRequest = { example: 'slice-data' };
            expect(readerFn).toHaveBeenCalledWith(sliceRequest, slice.logger, sliceRequest);

            const readerResults = times(10, () => 'hello');
            expect(opFn).toHaveBeenCalledWith(readerResults, slice.logger, sliceRequest);

            expect(results).toEqual(times(10, () => 'hi'));
        });
    });
});
