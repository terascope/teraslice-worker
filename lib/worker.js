'use strict';

const Job = require('./job');
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
            slicerUrl: formatURL(slicerHostname, slicerPort),
            clusterMasterUrl: formatURL(clusterMasterHostname, clusterMasterPort),
            workerId: this.workerId,
        });

        this.job = new Job(this.context, this.jobConfig);
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
        let config;

        try {
            config = this.messenger.onMessage('slicer:slice:new');
        } catch (err) {
            this._waitAndProcessSlice();
        }

        const slice = new Slice(this.context, this.jobConfig);
        await slice.initialize(this.job, config, this.stores);
    }
}

module.exports = Worker;
