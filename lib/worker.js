'use strict';

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
        this._shouldProcess = false;
        await this.stores.analyticsStore.shutdown();
        await this.stores.stateStore.shutdown();
        await this.messenger.close();
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

        this._waitAndProcessSlice();
    }

    async _processSlice(msg) {
        if (!this._shouldProcess) return;

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
}

module.exports = Worker;
