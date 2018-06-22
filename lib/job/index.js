'use strict';

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

    getOpConfig() { // eslint-disable-line class-methods-use-this
        // return execution.operations.find(op => op._op === name);
    }
}

module.exports = Job;
