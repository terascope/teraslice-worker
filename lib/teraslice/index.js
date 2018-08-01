'use strict';

const get = require('lodash/get');
const {
    utils,
    storage,
    validators,
    executionController,
    runners,
} = require('teraslice');

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
        config_schema: utils.system.configSchema,
        schema_formats: utils.schemaFormats,
        ops_directory: opsDirectory,
        cluster_name: clusterName,
        logging_connection: loggingConnection
    }, sysconfig);
}

async function validateJob(context, jobSpec) {
    const jobValidator = validators.job(context);
    try {
        return jobValidator.validate(jobSpec);
    } catch (error) {
        throw new Error(`validating job: ${error}`);
    }
}

async function convertJobToExecution(context, config, stores = {}) {
    const jobStore = stores.jobStore || await storage.job(context);
    const exStore = stores.exStore || await storage.execution(context);

    const validJob = await validateJob(context, config, { skipRegister: true });
    const jobSpec = await jobStore.create(config);

    const job = Object.assign({}, jobSpec, validJob);

    const ex = await exStore.create(job, 'ex');
    await exStore.setStatus(ex.ex_id, 'pending');

    if (!Object.keys(stores).length) {
        await Promise.all([
            exStore.shutdown(true),
            jobStore.shutdown(true),
        ]);
    }

    return {
        job,
        ex,
    };
}

module.exports = {
    getTerasliceConfig,
    convertJobToExecution,
    storage,
    validators,
    utils,
    executionController,
    runners,
    validateJob,
};
