'use strict';

const { EventEmitter } = require('events');
const { handleWorkerInput } = require('../../../lib/utils');
const { newJobConfig } = require('../../helpers');

describe('HandleWorkerInput', () => {
    let worker;
    beforeAll(() => {
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
        const jobConfig = newJobConfig();
        worker = handleWorkerInput(jobConfig, config);
    });

    it('should create a logger', () => {
        const logger = worker.makeLogger();
        expect(logger).toHaveProperty('flush');
        expect(logger).toHaveProperty('debug');
        expect(logger).toHaveProperty('info');
        expect(logger).toHaveProperty('trace');
        expect(logger).toHaveProperty('error');
        expect(logger).toHaveProperty('warn');
    });

    it('should have have workerId', () => {
        expect(worker).toHaveProperty('workerId');
        const { hostname } = worker.context.sysconfig.teraslice;
        const { id } = worker.context.cluster.worker;
        expect(worker.workerId).toEqual(`${hostname}__${id}`);
    });

    it('should have an event emitter', () => {
        expect(worker.events instanceof EventEmitter).toBe(true);
    });
});
