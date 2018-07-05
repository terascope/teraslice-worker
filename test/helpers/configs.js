'use strict';

const path = require('path');
const random = require('lodash/random');
const { newId } = require('../../lib/utils');

const opsPath = path.join(__dirname, '..', 'fixtures', 'ops');

const newSliceConfig = (request = { example: 'slice-data' }) => ({
    slice_id: newId('slice-id', true),
    slicer_id: newId('slicer-id', true),
    order: random(0, 1000),
    request,
    _created: new Date().toISOString()
});

const newJobConfig = (options = {}) => {
    const {
        analytics = false,
        assignment = 'worker',
        maxRetries = 0,
        slicerPort = 0,
        operations = [
            {
                _op: path.join(opsPath, 'example-reader'),
                exampleProp: 321
            },
            {
                _op: path.join(opsPath, 'example-op'),
                exampleProp: 123
            }
        ],
        assets = []
    } = options;
    return {
        assignment,
        job: {
            assets,
            analytics,
            max_retries: maxRetries,
            operations,
        },
        ex_id: newId('ex-id', true),
        job_id: newId('job-id', true),
        slicer_port: slicerPort,
        slicer_hostname: 'localhost'
    };
};


const newSysConfig = (options = {}) => {
    const {
        clusterName = 'test-teraslice-cluster',
        timeout = 3000,
        actionTimeout = 2000,
        assetDir,
        clusterMasterPort,
    } = options;

    return {
        terafoundation: {
            environment: 'development',
            connectors: {
                elasticsearch: {
                    default: {
                        host: ['127.0.0.1:9200'],
                        requestTimeout: timeout,
                        deadTimeout: timeout,
                    }
                }
            }
        },
        teraslice: {
            assets_directory: assetDir,
            shutdown_timeout: timeout,
            action_timeout: actionTimeout,
            network_latency_buffer: 0,
            slicer_timeout: timeout,
            slicer_allocation_attempts: 3,
            node_state_interval: timeout,
            node_disconnect_timeout: timeout,
            worker_disconnect_timeout: timeout,
            name: clusterName,
            master_hostname: 'localhost',
            port: clusterMasterPort,
        }
    };
};

module.exports = {
    opsPath,
    newJobConfig,
    newSliceConfig,
    newSysConfig
};
