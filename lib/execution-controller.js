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

    async initialize() {
        const { context } = this;

        const stateStore = makeStateStore(context);
        const exStore = makeExStore(context);

        this.stores.stateStore = await stateStore;
        this.stores.exStore = await exStore;

        await this.messenger.start();

        this.exectuionContext = await this.job.initialize();
    }

    async start() {
        const { context, logger, exectuionContext } = this;

        this.slicer = await exectuionContext.slicer.newSlicer(context, exectuionContext, undefined, logger);

        this._started = true;
    }

    async shutdown() {
        if (!this._started) return;

        await this.messenger.close();

        await Promise.map(Object.values(this.stores), (store) => {
            // attempt to shutdown but if it takes longer than shutdown_timeout, cleanup
            const forceShutdown = true;
            return store.shutdown(forceShutdown);
        });
    }
}

module.exports = ExecutionController;
