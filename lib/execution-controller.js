'use strict';

const get = require('lodash/get');
const forEach = require('lodash/forEach');
const isArray = require('lodash/isArray');
const Queue = require('@terascope/queue');
const ExecutionControllerMessenger = require('./messenger/execution-controller');
const Job = require('./job');
const { makeStateStore, makeExStore } = require('./teraslice');
const {
    generateWorkerId,
    validateJobConfig,
    makeLogger,
    newId
} = require('./utils');

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
        this.slicerQueue = new Queue();

        this.workerId = workerId;
        this.logger = logger;
        this.events = events;
        this.context = context;
        this.jobConfig = jobConfig;

        this.stores = {};
        this._started = false;
    }

    async initialize() {
        const { context, logger } = this;

        const stateStore = makeStateStore(context);
        const exStore = makeExStore(context);

        this.stores.stateStore = await stateStore;
        this.stores.exStore = await exStore;

        await this.messenger.start();

        const executionContext = await this.job.initialize();

        this.slicers = await executionContext.slicer.newSlicer(
            context,
            executionContext,
            undefined,
            logger
        );

        this.executionContext = executionContext;
    }

    async run() {
        const runForever = async () => {
            if (this._isShuttingDown) return;
            try {
                await this.runOnce();
            } catch (err) {
                this.logger.warn('Run failed but worker is not done processing');
            }
            await runForever();
        };

        this._started = true;
        await runForever();
    }

    async runOnce() {
        await this._createSlices();
    }

    async shutdown() {
        if (!this._started) return;

        const shutdownErrs = [];

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
            await this.messenger.close();
        } catch (err) {
            shutdownErrs.push(err);
        }

        this.stores = {};
        this.messenger = null;
        this.job = null;

        if (shutdownErrs.length) {
            const errMsg = shutdownErrs.map(e => e.toString()).join(', and');
            throw new Error(`Failed to shutdown correctly: ${errMsg}`);
        }
    }

    _createSlices() {
        const { logger } = this;
        // const { lifecycle } = this.executionContext.config;
        const { ex_id: exId } = this.jobConfig;
        let slicerOrder = 0;

        return Promise.map(this.slicers, async (slicerFn, slicerId) => {
            logger.trace(`slicer ${slicerId} is being called`);
            const sliceRequest = await slicerFn();
            if (sliceRequest != null) {
                if (isArray(sliceRequest)) {
                    logger.warn(`slicer for execution: ${exId} is subslicing by key`);
                    // executionAnalytics.increment('subslice_by_key');
                    forEach(sliceRequest, (request) => {
                        this._allocateSlice(request, slicerId, slicerOrder += 1);
                    });
                } else {
                    this._allocateSlice(sliceRequest, slicerId, slicerOrder += 1);
                }
            }
        });
    }

    _allocateSlice(sliceRequest, slicerId, slicerOrder) {
        const { logger, slicerQueue } = this;
        const { ex_id: exId } = this.jobConfig;
        const { stateStore } = this.stores;
        let slice = sliceRequest;
        // recovery slices already have correct meta data
        if (!sliceRequest.slice_id) {
            slice = {
                slice_id: newId(),
                request: sliceRequest,
                slicer_id: slicerId,
                slicer_order: slicerOrder,
                _created: new Date().toISOString()
            };

            stateStore.createState(exId, slice, 'start');
            logger.trace('enqueuing slice', slice);
        }
        slicerQueue.enqueue(slice);
    }
}

module.exports = ExecutionController;
