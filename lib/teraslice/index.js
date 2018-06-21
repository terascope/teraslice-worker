'use strict';

const configSchema = require('teraslice/lib/config/schemas/system').config_schema;
const schemaFormats = require('teraslice/lib/utils/convict_utils');

function opsDirectory(configFile) {
    if (configFile.teraslice && configFile.teraslice.ops_directory) {
        return configFile.teraslice.ops_directory;
    }
    return null;
}

function clusterName(configFile) {
    if (configFile.teraslice && configFile.teraslice.name) {
        return configFile.teraslice.name;
    }
    return null;
}

function loggingConnection(configFile) {
    if (configFile.teraslice && configFile.teraslice.state) {
        return configFile.teraslice.state.connection;
    }

    return 'default';
}

function getTerasliceConfig(config) {
    return Object.assign({
        name: 'teraslice',
        config_schema: configSchema,
        schema_formats: schemaFormats,
        ops_directory: opsDirectory,
        cluster_name: clusterName,
        logging_connection: loggingConnection
    }, config);
}

module.exports = {
    getTerasliceConfig
};

