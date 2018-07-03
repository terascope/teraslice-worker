'use strict';

const Promise = require('bluebird');
const get = require('lodash/get');
const Job = require('./job');
const Slice = require('./slice');
const WorkerMessenger = require('./messenger/worker');

const {
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('./teraslice');

const {
    formatURL,
    generateWorkerId,
    validateJobConfig,
    makeLogger,
} = require('./utils');

class Worker {
    constructor(context, jobConfig) {
        validateJobConfig(jobConfig);
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, jobConfig);
        const events = context.apis.foundation.getSystemEvents();

        const {
            master_hostname: clusterMasterHostname,
            port: clusterMasterPort,
        } = context.sysconfig.teraslice;

        const {
            slicer_port: slicerPort,
            slicer_hostname: slicerHostname
        } = jobConfig;

        const networkerLatencyBuffer = get(context, 'sysconfig.teraslice.network_latency_buffer');
        const actionTimeout = get(context, 'sysconfig.teraslice.action_timeout');
        const shutdownTimeout = get(context, 'sysconfig.teraslice.shutdown_timeout');

        this.messenger = new WorkerMessenger({
            executionControllerUrl: formatURL(slicerHostname, slicerPort),
            clusterMasterUrl: formatURL(clusterMasterHostname, clusterMasterPort),
            workerId,
            networkerLatencyBuffer,
            actionTimeout
        });

        this.slice = new Slice(context, jobConfig);
        this.job = new Job(context, jobConfig);

        this.stores = {};
        this.shutdownTimeout = shutdownTimeout;
        this.context = context;
        this.workerId = workerId;
        this.logger = logger;
        this.events = events;
        this._isShutdown = false;
        this._isShuttingDown = false;
        this._isProcessing = false;

        this._waitAndProcessSlice = this._waitAndProcessSlice.bind(this);
    }

    async start() {
        const { context } = this;

        this.stores.stateStore = await makeStateStore(context);
        this.stores.analyticsStore = await makeAnalyticsStore(context);

        await this.messenger.start();
        await this.messenger.ready();

        await this.job.initialize();

        this._waitAndProcessSlice();
    }

    async shutdown() {
        if (this._isShuttingDown) return;

        const shutdownErrs = [];
        this._isShuttingDown = true;

        this.events.emit('worker:shutdown');

        try {
            await this._waitForSliceToFinish();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.stores.analyticsStore.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.stores.stateStore.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.messenger.close();
        } catch (err) {
            shutdownErrs.push(err);
        }

        this._isShutdown = true;

        if (shutdownErrs.length) {
            const errMsg = shutdownErrs.map(e => e.toString()).join(', and');
            throw new Error(`Failed to shutdown correctly: ${errMsg}`);
        }
    }

    async _waitAndProcessSlice() {
        if (this._isShuttingDown) return;

        let msg;

        try {
            msg = await this.messenger.onMessage('slicer:slice:new');
        } catch (err) {
            this._waitAndProcessSlice();
            return;
        }

        this._isProcessing = true;

        try {
            await this._processSlice(msg);
        } catch (err) {
            this.logger.error(err);
        }

        this._isProcessing = false;

        this._waitAndProcessSlice();
    }

    async _processSlice(msg) {
        if (this._isShuttingDown) return;

        await this.slice.initialize(this.job, msg, this.stores);

        try {
            await this.slice.run();
        } catch (err) {
            await this.messenger.sliceComplete({
                slice: this.slice.slice,
                error: err.toString(),
            });
            return;
        }

        await this.messenger.sliceComplete(this.slice);
    }

    _waitForSliceToFinish() {
        if (!this._isProcessing) return Promise.resolve();

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
                if (this._isShutdown) {
                    logger.trace('worker shutdown during shutdown process');
                    done();
                    return;
                }

                if (!this._isProcessing) {
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
                err.code = 408;
                done(err);
            }, this.shutdownTimeout);
        });
    }
}

module.exports = Worker;
