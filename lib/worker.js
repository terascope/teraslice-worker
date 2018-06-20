'use strict';

const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const isNumber = require('lodash/isNumber');

const { makeContext } = require('./terafoundation');
const { getConfig } = require('./teraslice');

class Worker {
    constructor(config) {
        validateConfig(config);
        this.config = config;
    }

    start() { // eslint-disable-line class-methods-use-this

    }
}

function validateConfig(config) {
    if (!isPlainObject(config) || isEmpty(config)) throw new Error('Worker requires a valid configuration');
    const {
        assignment,
        job,
        exId,
        jobId,
        slicerPort
    } = config;
    if (!isString(assignment)) throw new Error('Worker configuration requires a valid assignment');
    if (!isPlainObject(job) || isEmpty(job)) throw new Error('Worker configuration requires a valid job');
    if (!isString(exId)) throw new Error('Worker configuration requires a valid exId');
    if (!isString(jobId)) throw new Error('Worker configuration requires a valid jobId');
    if (!isNumber(slicerPort)) throw new Error('Worker configuration requires a valid slicerPort');
}

function generateContext() {
    const config = getConfig({ name: 'teraslice-worker' });
    return makeContext(config);
}

module.exports = Worker;
module.exports.validateConfig = validateConfig;
module.exports.generateContext = generateContext;
