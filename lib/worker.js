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
    generateContext,
    generateWorkerId,
    validateJobConfig,
    makeLogger,
} = require('./utils');

class Worker {
    constructor(jobConfig, sysconfig, { timeoutMs } = { }) {
        validateJobConfig(jobConfig);
        this.context = generateContext(sysconfig);
        this.workerId = generateWorkerId(this.context);
        this.logger = makeLogger(this.context, jobConfig);
        this.events = this.context.apis.foundation.getSystemEvents();

        const {
            master_hostname: clusterMasterHostname,
            port: clusterMasterPort,
        } = this.context.sysconfig.teraslice;

        const {
            slicer_port: slicerPort,
            slicer_hostname: slicerHostname
        } = jobConfig;

        this.messenger = new WorkerMessenger({
            executionControllerUrl: formatURL(slicerHostname, slicerPort),
            clusterMasterUrl: formatURL(clusterMasterHostname, clusterMasterPort),
            workerId: this.workerId,
            timeoutMs,
        });

        this.slice = new Slice(this.context, jobConfig);
        this.job = new Job(this.context, jobConfig);

        this.stores = {};
        this.processing = false;
        this.shutdownTimeout = get(this.context, 'sysconfig.teraslice.shutdown_timeout');

        this._waitAndProcessSlice = this._waitAndProcessSlice.bind(this);
    }

    async start() {
        const { context } = this;
        this._shouldProcess = true;
        this.stores.stateStore = await makeStateStore(context);
        this.stores.analyticsStore = await makeAnalyticsStore(context);

        await this.messenger.start();
        await this.messenger.ready();

        await this.job.initialize();
        this._waitAndProcessSlice();
    }

    async shutdown() {
        let shutdownErr;
        this._shouldProcess = false;
        this.events.emit('worker:shutdown');

        try {
            await this._waitForSliceToFinish();
        } catch (err) {
            shutdownErr = err;
            this.logger.error(err);
        }

        await this.stores.analyticsStore.shutdown();
        await this.stores.stateStore.shutdown();
        await this.messenger.close();

        if (shutdownErr) {
            throw new Error(shutdownErr);
        }
    }

    async _waitAndProcessSlice() {
        let msg;

        try {
            msg = await this.messenger.onMessage('slicer:slice:new');
        } catch (err) {
            this._waitAndProcessSlice();
            return;
        }

        try {
            await this._processSlice(msg);
        } catch (err) {
            this.logger.error(err);
        }
        this.processing = false;

        this._waitAndProcessSlice();
    }

    async _processSlice(msg) {
        if (!this._shouldProcess) return;

        this.processing = true;

        await this.slice.initialize(this.job, msg, this.stores);
        try {
            await this.slice.run();
        } catch (err) {
            await this.messenger.sendToExecutionController('worker:slice:complete', {
                worker_id: this.workerId,
                slice: this.slice.slice,
                error: err.toString(),
            });
            return;
        }
        await this.messenger.sendToExecutionController('worker:slice:complete', {
            worker_id: this.workerId,
            slice: this.slice.slice,
            analytics: this.slice.analyticsData,
        });
    }

    _waitForSliceToFinish() {
        const { logger } = this;
        const startTime = Date.now();
        return new Promise((resolve, reject) => {
            let timeout;

            const interval = setInterval(() => {
                const elapsed = (Date.now() - startTime) / 1000;
                if (!this.processing) {
                    clearInterval(interval);
                    clearTimeout(timeout);
                    logger.trace(`is done with current slice, shutdown counter took ${elapsed} seconds`);
                    resolve();
                }
                if (elapsed % 60 === 0) {
                    logger.info(`shutdown sequence initiated, but is still processing. Will force shutdown in ${elapsed} seconds`);
                }
            }, 100);

            timeout = setTimeout(() => {
                clearInterval(interval);
                this.processing = false;
                const err = new Error(`Worker shutdown timeout after ${this.shutdownTimeout / 1000} seconds, forcing shutdown`);
                err.code = 408;
                reject(err);
            }, this.shutdownTimeout);
        });
    }
}

module.exports = Worker;
