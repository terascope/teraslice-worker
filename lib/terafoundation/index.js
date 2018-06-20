'use strict';

const validateConfigs = require('terafoundation/lib/validate_configs');
const { loggerClient } = require('terafoundation/lib/logger_utils');
const registerApis = require('terafoundation/lib/api');
const parseConfigFile = require('terafoundation/lib/sysconfig');

function getSysConfig(config) {
    const sysconfig = parseConfigFile({
        configfile: config.config_file
    });

    // allows top level function to declare ops_directory, so not hard baked in
    // TODO verify why we need this
    if (typeof config.ops_directory === 'function') {
        config.ops_directory = config.ops_directory(sysconfig);
    }

    return validateConfigs({ }, config, sysconfig);
}

function makeContext(config) {
    const context = {};
    let loggingConnection = 'default';

    context.sysconfig = getSysConfig(config);
    context.name = config.name || 'terafoundation';

    if (typeof config.cluster_name === 'function') {
        context.cluster_name = config.cluster_name(context.sysconfig);
    }

    if (typeof config.loggingConnection === 'function') {
        loggingConnection = config.loggingConnection(context.sysconfig);
    }

    // Initialize the API
    registerApis(context);
    delete context.apis.foundation.startWorkers;
    delete context.foundation.startWorkers;

    // Bootstrap the top level logger
    context.logger = context.apis.foundation.makeLogger(context.name, context.name);

    // FIXME: this should probably be refactored to actually create the
    // logger as it stands this function is very confusing
    loggerClient(context, context.logger, loggingConnection);

    return context;
}

module.exports = {
    makeContext,
};

