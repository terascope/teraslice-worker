'use strict';

const get = require('lodash/get');
const configSchema = require('teraslice/lib/config/schemas/system').config_schema;
const schemaFormats = require('teraslice/lib/utils/convict_utils');
const makeExStore = require('teraslice/lib/cluster/storage/execution');
const makeAssetStore = require('teraslice/lib/cluster/storage/assets');
const makeStateStore = require('teraslice/lib/cluster/storage/state');
const makeAnalyticsStore = require('teraslice/lib/cluster/storage/analytics');
const makeJobStore = require('teraslice/lib/cluster/storage/jobs');
const makeOpRunner = require('teraslice/lib/cluster/runners/op');
const makeJobValidator = require('teraslice/lib/config/validators/job');
const { dateFormat } = require('teraslice/lib/utils/date_utils');
const { saveAsset } = require('teraslice/lib/utils/file_utils');

function opsDirectory(configFile) {
    return get(configFile, 'teraslice.ops_directory', null);
}

function clusterName(configFile) {
    return get(configFile, 'teraslice.name', null);
}

function loggingConnection(configFile) {
    return get(configFile, 'teraslice.state.connection', 'default');
}

function getTerasliceConfig(sysconfig) {
    return Object.assign({
        name: 'teraslice',
        config_schema: configSchema,
        schema_formats: schemaFormats,
        ops_directory: opsDirectory,
        cluster_name: clusterName,
        logging_connection: loggingConnection
    }, sysconfig);
}

async function validateJob(context, jobSpec) {
    const jobValidator = makeJobValidator(context);
    try {
        return jobValidator.validate(jobSpec);
    } catch (error) {
        throw new Error(`validating job: ${error}`);
    }
}

module.exports = {
    getTerasliceConfig,
    makeExStore,
    makeStateStore,
    makeAnalyticsStore,
    makeAssetStore,
    makeJobStore,
    saveAsset,
    makeOpRunner,
    dateFormat,
    validateJob,
};
