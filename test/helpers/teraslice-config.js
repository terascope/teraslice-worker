'use strict';


module.exports = (options = {}) => {
    const {
        clusterName = 'test-teraslice-cluster',
        timeout = 5000,
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
            action_timeout: timeout,
            network_latency_buffer: timeout,
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
