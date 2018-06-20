'use strict';

const defaults = require('lodash/defaults');
// const {
//     validateConfigs,
//     loggerClient,
//     makeLogger,
//     getConnection,
//     getSystemEvents
// } = require('./terafoundation');
const {
    configSchema,
    schemaFormats,
    opsDirectory,
    clusterName,
    loggingConnection,
} = require('./teraslice');

class Context {
    constructor(config) {
        this.setConfig(config);
    }

    async generate() { // eslint-disable-line class-methods-use-this
        return {};
    }

    setConfig(config = {}) {
        this.config = defaults({
            name: 'teraslice-worker',
            config_schema: configSchema,
            schema_formats: schemaFormats,
            ops_directory: opsDirectory,
            cluster_name: clusterName,
            logging_connection: loggingConnection
        }, config);
        return this.config;
    }
}

module.exports = Context;
module.exports.opsDirectory = opsDirectory;
module.exports.clusterName = clusterName;
module.exports.loggingConnection = loggingConnection;
module.exports.configSchema = configSchema;
module.exports.schemaFormats = schemaFormats;
