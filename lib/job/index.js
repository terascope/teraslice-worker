'use strict';

const get = require('lodash/get');
const find = require('lodash/find');

class Job {
    constructor(context, jobConfig) {
        this.context = context;
        this.jobConfig = jobConfig;
        this.context.apis.registerAPI('job_runner', {
            getOpConfig: this.getOpConfig,
        });
    }

    initialize() { // eslint-disable-line class-methods-use-this

    }

    getOpConfig(name) {
        const operations = get(this.jobConfig, 'job.operations');
        return find(operations, { _op: name });
    }
}

module.exports = Job;
