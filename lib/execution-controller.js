'use strict';

const get = require('lodash/get');
const castArray = require('lodash/castArray');
const Queue = require('@terascope/queue');
const uuidv4 = require('uuid/v4');
const retry = require('bluebird-retry');
const Job = require('./job');
const ExecutionControllerMessenger = require('./messenger/execution-controller');
const ExecutionAnalytics = require('./execution-analytics');
const { makeStateStore, makeExStore } = require('./teraslice');
const makeEngine = require('./teraslice/engine');
const {
    generateWorkerId,
    validateJobConfig,
    makeLogger,
    WrapError,
} = require('./utils');

// TODO: Add _allSlicesProcessed

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

        this.executionAnalytics = new ExecutionAnalytics(context, jobConfig, this.messenger);

        this.slicerQueue = new Queue();

        this.workerId = workerId;
        this.logger = logger;
        this.events = events;
        this.context = context;
        this.jobConfig = jobConfig;
        this.exId = jobConfig.ex_id;
        this.stores = {};

        this.slicerDone = 0;
        this.queueLength = 0;
        this.isRecovery = false;
        this.isShuttingDown = false;
        this.isProcessing = false;
        this.isInitialized = false;

        this._dispatchSlices = this._dispatchSlices.bind(this);
    }

    async initialize() {
        const { context } = this;
        this.isInitialized = true;

        const stateStore = makeStateStore(context);
        const exStore = makeExStore(context);

        this.stores.stateStore = await stateStore;
        this.stores.exStore = await exStore;

        await this.messenger.start();

        this.messenger.on('worker:online', () => {
            this._adjustSlicerQueueLength();
        });

        this.messenger.on('worker:offline', () => {
            this._adjustSlicerQueueLength();
        });

        this.executionContext = await this.job.initialize();

        this.engine = makeEngine(this);

        await this._slicerInit();

        this.isInitialized = true;
    }

    async _slicerInit() {
        const { logger, context, executionContext } = this;

        const maxRetries = get(this.jobConfig, 'job.max_retries', 3);
        const retryOptions = {
            max_tries: maxRetries,
            throw_original: true,
            interval: 100,
        };

        this.slicers = await retry(() => {
            const startingPointStateRecords = undefined; // FIXME
            return this.executionContext.slicer.newSlicer(
                context,
                executionContext,
                startingPointStateRecords,
                logger
            );
        }, retryOptions);
    }

    async run() {
        const { exId, logger } = this;
        const { exStore } = this.stores;

        this.isProcessing = true;

        await this.executionAnalytics.start();

        this.queueLength = this.engine.setQueueLength(this.executionContext);

        logger.info(`Setting slicer queue length to ${this.queueLength}`);

        this.scheduler = this.engine.registerSlicers(this.slicers, this.isRecovery);

        await exStore.setStatus(exId, 'running');

        return Promise.all([
            this._doneProcessing(),
            this._processSlices(),
        ]);
    }

    async shutdown() {
        if (!this.isInitialized) return;
        if (this.isShuttingDown) return;

        this.isShuttingDown = true;
        this.isProcessing = false;

        const shutdownErrs = [];

        try {
            await this._doneProcessing();
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
            await this.engine.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.executionAnalytics.shutdown();
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

    async allocateSlice(request, slicerId, startingOrder) {
        let slicerOrder = startingOrder;
        const { logger, slicerQueue, exId } = this;
        const { stateStore } = this.stores;

        await Promise.map(castArray(request), async (sliceRequest) => {
            slicerOrder += 1;
            let slice = sliceRequest;

            // recovery slices already have correct meta data
            if (!slice.slice_id) {
                slice = {
                    slice_id: uuidv4(),
                    request: sliceRequest,
                    slicer_id: slicerId,
                    slicer_order: slicerOrder,
                    _created: new Date().toISOString()
                };

                await stateStore.createState(exId, slice, 'start');
                logger.trace('enqueuing slice', slice);
            }

            slicerQueue.enqueue(slice);
        });

        return slicerOrder;
    }

    async setFailingStatus() {
        const { exId } = this;
        const { exStore } = this.stores;

        const errMsg = `slicer: ${exId} has encountered a processing_error`;
        this.logger.error(errMsg);

        const executionStats = this.executionAnalytics.getAnalytics();
        const errorMeta = exStore.executionMetaData(executionStats, errMsg);
        exStore.setStatus(exId, 'failing', errorMeta);
    }

    async slicerFailure(err) {
        const { exId } = this;
        const { exStore } = this.stores;

        this.isProcessing = false;

        const error = new WrapError(`slicer for ex ${exId} had an error, shutting down execution`, err);
        this.logger.error(error.toString());

        const executionStats = this.executionAnalytics.getAnalytics();
        const errorMeta = exStore.executionMetaData(executionStats, error.toString());

        await exStore.setStatus(exId, 'failed', errorMeta);

        await this.messenger.executionTerminal(exId);
    }

    async slicerCompleted() {
        this.slicerDone += 1;

        this.logger.info(`a slicer for execution: ${this.exId} has completed its range`);

        if (this._slicersDone()) {
            this.events.emit('slicers:finished');
        }
    }

    _slicersDone() {
        return this.slicerDone === this.slicers.length;
    }

    async _adjustSlicerQueueLength() {
        const dynamicQueueLength = this.engine.hasDynamicQueueLength();
        if (!dynamicQueueLength) return;

        const clientsOnline = await this.messenger.getClientCounts();

        if (clientsOnline > this.queueLength) {
            this.queueLength = clientsOnline;
            this.logger.info(`adjusted queue length ${this.queueLength}`);
        }
    }

    async _processSlices() {
        if (this._isShuttingDown) return;
        if (this._slicersDone()) return;
        if (!this.isProcessing) return;

        try {
            // If all slicers are not done, the slicer queue is not overflown and the scheduler
            // is set, then attempt to provision more slices
            if (this.scheduler && this.slicerQueue.size() < this.queueLength) {
                await Promise.map(this.scheduler, slicerFn => slicerFn());
            }
            await this._dispatchSlices();
        } catch (err) {
            this.logger.warn('Run failed but worker is not done processing');
        }
        await this._processSlices();
    }

    _doneProcessing() { // TODO: this is not complete
        const { events } = this;

        return new Promise((resolve) => {
            const id = setInterval(() => {
                if (!this.isProcessing || this.isShuttingDown) {
                    slicersFinished();
                }
            }, 100).unref(); // unref so we don't stop the process from shutting down

            function slicersFinished() {
                clearInterval(id);
                events.removeListener('slicers:finished', slicersFinished);
                resolve();
            }

            events.on('slicers:finished', slicersFinished);
        });
    }

    async _dispatchSlices() {
        if (!this.slicerQueue.size()) return false;
        if (!this.isProcessing) return false;

        if (!this.messenger.availableWorkers()) {
            try {
                await this.messenger.onceWithTimeout('worker:enqueue');
            } catch (err) {
                return this._dispatchSlices();
            }
        }

        const slice = this.slicerQueue.dequeue();

        this.logger.debug(`dispatching slice ${JSON.stringify(slice)}`);

        await this.messenger.dispatchSlice(slice);
        return this._dispatchSlices();
    }
}

module.exports = ExecutionController;
