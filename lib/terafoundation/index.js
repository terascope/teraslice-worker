'use strict';

const validateConfigs = require('terafoundation/lib/validate_configs');
const { loggerClient } = require('terafoundation/lib/logger_utils');
const makeLogger = require('terafoundation/lib/api/makeLogger');
const getConnection = require('terafoundation/lib/api/getConnection');
const getSystemEvents = require('terafoundation/lib/api/getSystemEvents');

module.exports = {
    validateConfigs,
    loggerClient,
    makeLogger,
    getConnection,
    getSystemEvents,
};

