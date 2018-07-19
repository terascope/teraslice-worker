'use strict';

const get = require('lodash/get');
const Job = require('./job');
const ExecutionRunner = require('./teraslice/execution-runner');
const Slice = require('./slice');
const WorkerMessenger = require('./messenger/worker');

const {
    makeStateStore,
    makeAnalyticsStore
} = require('./teraslice');

const {
    formatURL,
    generateWorkerId,
    validateJobConfig,
    makeLogger,
} = require('./utils');

class Worker {
    constructor(context, jobConfig, useExecutionRunner = false) {
        validateJobConfig(jobConfig);
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, jobConfig);
        const events = context.apis.foundation.getSystemEvents();

        const {
            slicer_port: slicerPort,
            slicer_hostname: slicerHostname
        } = jobConfig;

        const networkerLatencyBuffer = get(context, 'sysconfig.teraslice.network_latency_buffer');
        const actionTimeout = get(context, 'sysconfig.teraslice.action_timeout');
        const shutdownTimeout = get(context, 'sysconfig.teraslice.shutdown_timeout');

        this.messenger = new WorkerMessenger({
            executionControllerUrl: formatURL(slicerHostname, slicerPort),
            workerId,
            networkerLatencyBuffer,
            actionTimeout
        });

        this.slice = new Slice(context, jobConfig);

        if (useExecutionRunner) {
            this.job = new ExecutionRunner(context, jobConfig);
        } else {
            this.job = new Job(context, jobConfig);
        }

        this.stores = {};
        this.shutdownTimeout = shutdownTimeout;
        this.context = context;
        this.workerId = workerId;
        this.logger = logger;
        this.events = events;

        this.isShuttingDown = false;
        this.isProcessing = false;
        this.isInitialized = false;
        this.slicesProcessed = 0;
    }

    async initialize() {
        const { context } = this;
        this.isInitialized = true;

        const stateStore = makeStateStore(context);
        const analyticsStore = makeAnalyticsStore(context);
        this.stores.stateStore = await stateStore;
        this.stores.analyticsStore = await analyticsStore;

        await this.messenger.start();

        this.executionContext = await this.job.initialize();

        await this.messenger.ready();
    }

    async run() {
        const runForever = async () => {
            if (this.isShuttingDown) return;
            try {
                await this.runOnce();
            } catch (err) {
                this.logger.warn('Slice failed but worker is not done processing');
            }
            await runForever();
        };

        await runForever();
    }

    async runOnce() {
        const msg = await this.messenger.waitForSlice(() => this.isShuttingDown);

        if (!msg) return;

        this.isProcessing = true;

        try {
            await this.slice.initialize(this.executionContext, msg, this.stores);

            await this.slice.run();

            await this.messenger.sliceComplete({
                slice: this.slice.slice,
                analytics: this.slice.analyticsData,
            });
        } catch (err) {
            this.logger.error(err);

            await this.messenger.sliceComplete({
                slice: this.slice.slice,
                analytics: this.slice.analyticsData,
                error: err.toString(),
            });
        }

        this.isProcessing = false;
        this.slicesProcessed += 1;
    }

    async shutdown() {
        if (!this.isInitialized) return;
        if (this.isShuttingDown) return;

        this.isShuttingDown = true;

        const shutdownErrs = [];

        this.events.emit('worker:shutdown');

        try {
            await this._waitForSliceToFinish();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await Promise.map(Object.values(this.stores), (store) => {
                // attempt to shutdown but if it takes longer than shutdown_timeout, cleanup
                const forceShutdown = true;
                return store.shutdown(forceShutdown);
            });
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.job.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.slice.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.messenger.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        this.stores = {};
        this.executionContext = null;

        if (shutdownErrs.length) {
            const errMsg = shutdownErrs.map(e => e.stack).join(', and');
            throw new Error(`Failed to shutdown correctly: ${errMsg}`);
        }
    }

    _waitForSliceToFinish() {
        if (!this.isProcessing) return Promise.resolve();

        const { logger } = this;
        const startTime = Date.now();

        return new Promise((resolve, reject) => {
            let timeout;
            let interval;
            const done = (err) => {
                clearInterval(interval);
                clearTimeout(timeout);
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            };

            interval = setInterval(() => {
                const elapsed = (Date.now() - startTime) / 1000;
                if (!this.isProcessing) {
                    logger.trace(`is done with current slice, shutdown counter took ${elapsed} seconds`);
                    done();
                    return;
                }

                if (elapsed % 60 === 0) {
                    logger.info(`shutdown sequence initiated, but is still processing. Will force shutdown in ${elapsed} seconds`);
                }
            }, 100);

            timeout = setTimeout(() => {
                const err = new Error(`Worker shutdown timeout after ${this.shutdownTimeout / 1000} seconds, forcing shutdown`);
                done(err);
            }, this.shutdownTimeout);
        });
    }
}

module.exports = Worker;
