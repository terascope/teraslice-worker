'use strict';

const Worker = require('../..');

describe('Worker Assignment', () => {
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
            assignment: 'worker',
            job: {
                example: true
            },
            exId: 'example-ex-id',
            jobId: 'example-job-id',
            slicerPort: 0
        };
        worker = new Worker(config, jobConfig);
    });

    describe('->start', () => {
        beforeEach(() => worker.start());

        it('should create a logger', () => {
            expect(worker.context).toHaveProperty('logger');
            expect(worker.context.logger).toHaveProperty('flush');
            expect(worker.context.logger).toHaveProperty('debug');
            expect(worker.context.logger).toHaveProperty('info');
            expect(worker.context.logger).toHaveProperty('trace');
            expect(worker.context.logger).toHaveProperty('error');
            expect(worker.context.logger).toHaveProperty('warn');
        });
    });
});
