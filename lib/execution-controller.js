'use strict';

const {
    generateWorkerId,
    validateJobConfig,
    makeLogger,
} = require('./utils');

class ExecutionController {
    constructor(context, jobConfig) {
        validateJobConfig(jobConfig);
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, jobConfig);
        const events = context.apis.foundation.getSystemEvents();

        this.workerId = workerId;
        this.logger = logger;
        this.events = events;
        this.context = context;
        this.jobConfig = jobConfig;
    }

    start() { // eslint-disable-line class-methods-use-this

    }

    shutdown() { // eslint-disable-line class-methods-use-this

    }
}

module.exports = ExecutionController;
