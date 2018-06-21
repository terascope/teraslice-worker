'use strict';

const os = require('os');
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
            expect(worker).toHaveProperty('logger');
            expect(worker.logger).toHaveProperty('flush');
            expect(worker.logger).toHaveProperty('debug');
            expect(worker.logger).toHaveProperty('info');
            expect(worker.logger).toHaveProperty('trace');
            expect(worker.logger).toHaveProperty('error');
            expect(worker.logger).toHaveProperty('warn');
        });

        it('should have have workerId', () => {
            expect(worker).toHaveProperty('workerId');
            const { hostname } = worker.context.sysconfig.teraslice;
            expect(worker.workerId).toContain(hostname);
        });
    });
});
