'use strict';

const { generateContext } = require('../../');

describe('Terafoundation Context', () => {
    let context;

    beforeEach(() => {
        context = generateContext({
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
        });
    });

    it('should throw an error when given no config', () => {
        expect(() => { generateContext(); }).toThrowError('Worker requires a valid terafoundation configuration');
    });

    it('should throw an error when given an invalid config', () => {
        expect(() => { generateContext('hello'); }).toThrowError('Worker requires a valid terafoundation configuration');
    });

    it('should have the correct apis', () => {
        expect(context.apis.foundation).toHaveProperty('makeLogger');
        expect(context.foundation).toHaveProperty('makeLogger');
        expect(context.apis.foundation).toHaveProperty('getSystemEvents');
        expect(context.foundation).toHaveProperty('getEventEmitter');
        expect(context.apis.foundation).toHaveProperty('getConnection');
        expect(context.foundation).toHaveProperty('getConnection');
        expect(context.apis.foundation).not.toHaveProperty('startWorkers');
        expect(context.foundation).not.toHaveProperty('startWorkers');
        expect(context.apis).toHaveProperty('registerAPI');
    });

    it('should have the correct metadata', () => {
        expect(context).toHaveProperty('name', 'teraslice-worker');
        expect(context.sysconfig).toHaveProperty('teraslice');
        expect(context.sysconfig).toHaveProperty('terafoundation');
        expect(context.sysconfig.teraslice).toMatchObject({
            shutdown_timeout: 60000,
            master_hostname: 'localhost',
            port: 5678,
            name: 'test-teraslice-cluster',
            action_timeout: 300000,
            network_latency_buffer: 15000,
            slicer_timeout: 180000,
            slicer_allocation_attempts: 3,
            node_state_interval: 5000,
            node_disconnect_timeout: 300000,
            worker_disconnect_timeout: 300000,
            slicer_port_range: '45679:46678',
            analytics_rate: 60000,
            cluster_manager_type: 'native'
        });
        expect(context.sysconfig).toHaveProperty('terafoundation');
        expect(context.sysconfig.terafoundation).toMatchObject({
            environment: 'development',
            log_path: process.cwd(),
            log_level: 'info',
            log_buffer_limit: 30,
            log_buffer_interval: 60000,
            log_index_rollover_frequency: 'monthly',
        });
    });


    it('should have a logger', () => {
        expect(context).toHaveProperty('logger');
    });
});

