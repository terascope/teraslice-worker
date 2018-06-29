'use strict';

const Job = require('./job');
const Slice = require('./slice');
const BaseWorker = require('./base-worker');
const WorkerMessenger = require('./messenger/worker');
const {
    stateStore: makeStateStore,
    analyticsStore: makeAnalyticsStore
} = require('./teraslice');
const { formatURL } = require('./utils');

class Worker extends BaseWorker {
    constructor(config, jobConfig) {
        super(config, jobConfig);
        this.stores = {};
        this.makeLogger();

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
        });

        this.slice = new Slice(config, jobConfig);
        this.job = new Job(this.context, jobConfig);
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
        if (this.stores.analyticsStore) {
            await this.stores.analyticsStore.shutdown();
        }
        if (this.stores.stateStore) {
            await this.stores.stateStore.shutdown();
        }
        await this.messenger.close();
    }

    async _waitAndProcessSlice() {
        let msg;

        try {
            msg = await this.messenger.onMessage('slicer:slice:new');
        } catch (err) {
            // if timeout continue waiting for new slice message
            if (err.code === 408) {
                this._waitAndProcessSlice();
                return;
            }
            throw err;
        }

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
