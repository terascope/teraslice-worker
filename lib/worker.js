'use strict';

const _ = require('lodash');

class Worker {
    constructor(config) {
        validateConfig(config);
        this.config = config;
    }

    start() {

    }
}

function validateConfig(config) {
    if (!_.isPlainObject(config) || _.isEmpty(config)) throw new Error('Worker requires a valid configuration');
    const {
        assignment,
        job,
        exId,
        jobId,
        slicerPort
    } = config;
    if (!_.isString(assignment)) throw new Error('Worker configuration requires a valid assignment');
    if (!_.isPlainObject(job) || _.isEmpty(job)) throw new Error('Worker configuration requires a valid job');
    if (!_.isString(exId)) throw new Error('Worker configuration requires a valid exId');
    if (!_.isString(jobId)) throw new Error('Worker configuration requires a valid jobId');
    if (!_.isNumber(slicerPort)) throw new Error('Worker configuration requires a valid slicerPort');
}

module.exports = Worker;
module.exports.validateConfig = validateConfig;
