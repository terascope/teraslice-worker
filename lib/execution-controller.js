'use strict';

const { makeAnalyticsStore, makeStateStore } = require('./teraslice');
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

        this.stores = {};
        this._started = false;
    }

    async start() {
        const { context } = this;

        this.stores.stateStore = await makeStateStore(context);
        this.stores.analyticsStore = await makeAnalyticsStore(context);
        this._started = true;
    }

    async shutdown() {
        if (!this._started) return;

        await this.stores.analyticsStore.destroy();
        await this.stores.stateStore.destroy();
    }
}

module.exports = ExecutionController;
