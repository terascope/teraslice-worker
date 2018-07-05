'use strict';

const get = require('lodash/get');
const ExecutionControllerMessenger = require('./messenger/execution-controller');
const Job = require('./job');
const { makeStateStore, makeExStore } = require('./teraslice');
const { generateWorkerId, validateJobConfig, makeLogger } = require('./utils');

class ExecutionController {
    constructor(context, jobConfig) {
        validateJobConfig(jobConfig);
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, jobConfig);
        const events = context.apis.foundation.getSystemEvents();

        const port = jobConfig.slicer_port;
        const networkerLatencyBuffer = get(context, 'sysconfig.teraslice.network_latency_buffer');
        const actionTimeout = get(context, 'sysconfig.teraslice.action_timeout');

        this.messenger = new ExecutionControllerMessenger({
            port,
            networkerLatencyBuffer,
            actionTimeout
        });

        this.job = new Job(context, jobConfig);

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
        this.stores.exStore = await makeExStore(context);

        await this.messenger.start();
        await this.job.initialize();

        this._started = true;
    }

    async shutdown() {
        if (!this._started) return;

        await this.stores.stateStore.shutdown(true);
        await this.stores.exStore.shutdown(true);
    }
}

module.exports = ExecutionController;
