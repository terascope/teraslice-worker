'use strict';

const path = require('path');
const Slice = require('../../../lib/worker/slice');
const Job = require('../../../lib/job');
const TestContext = require('../../helpers/test-context');

const opsPath = path.join(__dirname, '..', '..', 'fixtures', 'ops');
const exampleReaderMock = require(path.join(opsPath, 'example-reader')).newReader;
const exampleOpMock = require(path.join(opsPath, 'example-op')).newProcessor;

describe('Slice', () => {
    let slice;
    let _testContext;

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
        exampleReaderMock.mockResolvedValue(jest.fn());
        exampleOpMock.mockResolvedValue(jest.fn());

        const job = new Job(_testContext.context, jobConfig);
        const executionApi = await job.initialize();

        slice = new Slice(_testContext.config, jobConfig);
        await slice.run(executionApi);
    });

    it('should have a run method', () => {
        expect(slice.run).toBeFunction();
    });
});
