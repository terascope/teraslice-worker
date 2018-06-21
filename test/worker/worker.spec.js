'use strict';

const { Worker, TerasliceWorker } = require('../..');
const overrideLogger = require('../helpers/override-logger');
// const elasticsearchMock = require('@terascope/elasticsearch-api')

jest.mock('@terascope/elasticsearch-api');

describe('Worker', () => {
    let worker;
    beforeEach(() => {
        const config = {
            terafoundation: {
                environment: 'development',
                connectors: {
                    elasticsearch: {
                        default: {
                            host: ['example.dev:9200']
                        }
                    }
                }
            },
            teraslice: {
                name: 'test-teraslice-cluster',
                master_hostname: 'localhost'
            }
        };
        const jobConfig = {
            type: 'worker',
            job: {
                example: true
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        };
        worker = new Worker(config, jobConfig);
        overrideLogger(worker);
    });

    it('should be an instance of TerasliceWorker', () => {
        expect(worker instanceof TerasliceWorker).toBe(true);
    });

    it('should create the correct stores', async () => {
        await expect(worker.start()).resolves.toBeUndefined();
        expect(worker).toHaveProperty('assetStore');
        expect(worker.assetStore).toHaveProperty('shutdown');
        expect(worker).toHaveProperty('stateStore');
        expect(worker.stateStore).toHaveProperty('shutdown');
        expect(worker).toHaveProperty('analyticsStore');
        expect(worker.analyticsStore).toHaveProperty('shutdown');
        await expect(worker.shutdown()).resolves.toBeUndefined();
    });
});
