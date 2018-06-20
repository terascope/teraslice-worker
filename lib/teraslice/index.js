'use strict';

const defaults = require('lodash/defaults');
const configSchema = require('teraslice/lib/config/schemas/system').config_schema;
const schemaFormats = require('teraslice/lib/utils/convict_utils');
const yargs = require('yargs');

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

function getConfig(config) {
    const { argv } = yargs
        .usage('Usage: $0 [options]')
        .alias('c', 'configfile')
        .describe('c', `Configuration file to load.
            If not specified, the envorinment TERAFOUNDATION_CONFIG can be used.`)
        .alias('b', 'bootstrap')
        .describe('b', 'Perform initial setup')
        .help('h')
        .alias('h', 'help');

    return defaults({
        config_file: argv.configfile,
        name: 'teraslice',
        config_schema: configSchema,
        schema_formats: schemaFormats,
        ops_directory: opsDirectory,
        cluster_name: clusterName,
        logging_connection: loggingConnection
    }, config);
}

module.exports = {
    getConfig
};

