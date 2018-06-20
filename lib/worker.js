'use strict';

const _ = require('lodash');

class Worker {
    constructor(config) {
        validConfig(config);
        this.config = config;
    }

    start() {

    }
}

function validConfig(config) {
    if (!_.isPlainObject(config) || _.isEmpty(config)) throw new Error('Worker requires a valid configuration');
    const { assignment, job } = config;
    if (!_.isString(assignment)) throw new Error('Worker configuration requires a valid assignment');
    if (!_.isPlainObject(job) || _.isEmpty(job)) throw new Error('Worker configuration requires a valid job');
}

module.exports = Worker;
