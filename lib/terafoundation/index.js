'use strict';

const validateConfigs = require('terafoundation/lib/validate_configs');
const { loggerClient } = require('terafoundation/lib/logger_utils');
const registerApis = require('terafoundation/lib/api');

function makeContext(cluster, config, sysconfig) {
    const context = {};
    let loggingConnection = 'default';
    context.sysconfig = validateConfigs(cluster, config, sysconfig);
    context.name = config.name || 'terafoundation';
    context.cluster = cluster;

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
