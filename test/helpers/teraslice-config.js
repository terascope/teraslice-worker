'use strict';

module.exports = ({ clusterName = 'test-teraslice-cluster', timeout = 5000 } = {}) => ({
    terafoundation: {
        environment: 'development',
        connectors: {
            elasticsearch: {
                default: {
                    host: ['localhost:9200']
                }
            }
        }
    },
    teraslice: {
        shutdown_timeout: timeout,
        action_timeout: timeout,
        network_latency_buffer: timeout,
        slicer_timeout: timeout,
        slicer_allocation_attempts: 3,
        node_state_interval: timeout,
        node_disconnect_timeout: timeout,
        worker_disconnect_timeout: timeout,
        name: clusterName,
        master_hostname: 'localhost'
    }
});

