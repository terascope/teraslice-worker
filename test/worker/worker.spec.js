'use strict';

const { Worker, TerasliceWorker } = require('../..');

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
    });

    it('should be an instance of TerasliceWorker', () => {
        expect(worker instanceof TerasliceWorker).toBe(true);
    });

    describe('->start', () => {
        beforeEach(() => worker.start());

        afterEach(() => worker.shutdown());

        it('should get here', () => {});
    });
});
