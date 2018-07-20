'use strict';

const get = require('lodash/get');
const castArray = require('lodash/castArray');
const uuidv4 = require('uuid/v4');
const retry = require('bluebird-retry');
const Queue = require('@terascope/queue');
const parseError = require('@terascope/error-parser');

const Job = require('./job');
const ExecutionControllerMessenger = require('./messenger/execution-controller');
const ExecutionAnalytics = require('./execution-analytics');
const {
    makeStateStore,
    makeExStore,
    makeSliceAnalytics,
    newFormatedDate
} = require('./teraslice');
const makeEngine = require('./teraslice/engine');
const {
    generateWorkerId,
    validateJobConfig,
    makeLogger,
    WrapError,
} = require('./utils');

// TODO: Handle worker:slice:complete
// TODO: Handle recovery
// TODO: Handle worker disconnects and reconnects

class ExecutionController {
    constructor(context, jobConfig) {
        validateJobConfig(jobConfig);
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, jobConfig, 'execution_controller');
        const events = context.apis.foundation.getSystemEvents();

        const port = jobConfig.slicer_port;
        const networkerLatencyBuffer = get(context, 'sysconfig.teraslice.network_latency_buffer');
        const actionTimeout = get(context, 'sysconfig.teraslice.action_timeout');

        this.messenger = new ExecutionControllerMessenger({
            port,
            networkerLatencyBuffer,
            actionTimeout,
            events,
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
        this.jobId = jobConfig.job_id;
        this.stores = {};

        this.slicerDone = 0;
        this.queueLength = 0;
        this.isRecovery = false;
        this.isShuttingDown = false;
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

        this.events.on('slice:success', (response) => {
            if (this.jobConfig.job.analytics) {
                this.slicerAnalytics.addStats(response.analytics);
            }
            this.logger.debug(`worker ${response.worker_id} has completed its slice,`, response);
        });

        this.events.on('slice:failed', (response) => {
            this.logger.error(`worker: ${response.worker_id} has error on slice: ${JSON.stringify(response)} , slicer: ${this.exId}`);
            this.executionAnalytics.increment('failed');
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

        logger.debug(`initialized ${this.slicers.length} slices`);
    }

    async run() {
        const {
            exId,
            jobId,
            logger,
            context,
            executionContext
        } = this;
        const { exStore } = this.stores;

        this.startTime = Date.now();

        this.slicerAnalytics = makeSliceAnalytics(context, executionContext, {
            exId,
            jobId,
        });

        await this.executionAnalytics.start();

        this.queueLength = this.engine.setQueueLength(executionContext);

        logger.info(`Setting slicer queue length to ${this.queueLength}`);

        this.scheduler = this.engine.registerSlicers(this.slicers, this.isRecovery);

        await exStore.setStatus(exId, 'running');

        return Promise.all([
            this._processSlices(),
            this._doneProcessing(),
        ]);
    }

    async shutdown() {
        if (!this.isInitialized) return;
        if (this.isShuttingDown) return;
        const isDone = this.isDone();

        this.isShuttingDown = true;
        this.logger.info('execution controller is shutting down...');

        const shutdownErrs = [];

        if (!isDone) {
            try {
                await this._doneProcessing();
            } catch (err) {
                shutdownErrs.push(err);
            }
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
            await this.messenger.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        this.stores = {};
        this.messenger = null;
        this.job = null;

        if (shutdownErrs.length) {
            const errMsg = shutdownErrs.map(e => e.stack).join(', and');
            throw new Error(`Failed to shutdown correctly: ${errMsg}`);
        }

        this.logger.debug('execution controller is shutdown');
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
        const errorMeta = await exStore.executionMetaData(executionStats, errMsg);
        await exStore.setStatus(exId, 'failing', errorMeta);
    }

    async slicerFailure(err) {
        const { exId } = this;
        const { exStore } = this.stores;

        this.stopProcessing = true;
        const error = new WrapError(`slicer for ex ${exId} had an error, shutting down execution`, err);
        this.logger.error(error.toString());

        const executionStats = this.executionAnalytics.getAnalytics();
        const errorMeta = await exStore.executionMetaData(executionStats, error.toString());

        await exStore.setStatus(exId, 'failed', errorMeta);

        // FIXME: await this.clusterMaster.executionTerminal(exId);
    }

    async slicerCompleted() {
        this.slicerDone += 1;

        this.logger.info(`a slicer for execution: ${this.exId} has completed its range`);

        if (!this._slicersDone()) return;

        this.stopProcessing = true;

        this.events.emit('slicers:finished');

        this.logger.info(`all slicers for execution: ${this.exId} have been completed, waiting for slices in slicerQueue to be processed`);
        this.executionAnalytics.set('queuing_complete', newFormatedDate());
    }

    isDone() {
        return this.isShuttingDown || this.stopProcessing || this._slicersDone();
    }

    _slicersDone() {
        return this.slicerDone === this.slicers.length;
    }

    async _adjustSlicerQueueLength() {
        const dynamicQueueLength = this.engine.hasDynamicQueueLength();
        if (!dynamicQueueLength) return;

        const clientsOnline = this.messenger.connectedWorkers();

        if (clientsOnline > this.queueLength) {
            this.queueLength = clientsOnline;
            this.logger.info(`adjusted queue length ${this.queueLength}`);
        }
    }

    _logFinishedJob() {
        const endTime = Date.now();
        const time = (endTime - this.startTime) / 1000;

        this.executionAnalytics.set('job_duration', time);

        if (this.jobConfig.job.analytics) {
            this.slicerAnalytics.analyzeStats();
        }

        this.logger.info(`execution ${this.exId} has finished in ${time} seconds`);
    }

    async _processSlices() {
        if (this.isDone()) return;

        const {
            slicerQueue,
            messenger,
            executionAnalytics
        } = this;

        try {
            await this._createSlices();
            await this._dispatchSlices();
        } catch (err) {
            const error = new WrapError('Run failed but worker is not done processing', err);
            this.logger.warn(error.toString());
        }

        executionAnalytics.set('workers_available', messenger.connectedWorkers());
        executionAnalytics.set('queued', slicerQueue.size());
        executionAnalytics.set('workers_active', messenger.activeWorkers());

        await Promise.delay(0);

        await this._processSlices();
    }

    async _doneProcessing() {
        const { events } = this;

        await new Promise((resolve) => {
            const id = setInterval(() => {
                if (this.isDone()) {
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

        await this._waitForSlicesToComplete();

        try {
            await this._executionCompleted();
        } catch (err) {
            const errMsg = parseError(err);
            this.logger.error(`execution ${this.exId} has run to completion but the process has failed while updating the execution status, slicer will soon exit, error: ${errMsg}`);

            await Promise.delay(100);

            this.events.emit('execution:shutdown');
        }

        await this._logFinishedJob();
    }

    async _dispatchSlices() {
        if (this.isDone()) return false;
        if (!this.slicerQueue.size()) return false;

        if (!this.messenger.availableWorkers()) {
            try {
                await this.messenger.onceWithTimeout('worker:enqueue');
            } catch (err) {
                return this._dispatchSlices();
            }
        }

        const slice = this.slicerQueue.dequeue();


        const { dispatched, workerId } = await this.messenger.dispatchSlice(slice);

        if (!dispatched) {
            this.slicerQueue.enqueue(slice);
            this.logger.debug(`worker ${workerId} not available to process slice ${slice.slice_id}`);
            return false;
        }

        this.logger.debug(`dispatched slice ${slice.slice_id} to worker ${workerId}`);

        return this._dispatchSlices();
    }

    async _createSlices() {
        if (!this.scheduler) return;
        if (this._slicersDone()) return;

        // If all slicers are not done, the slicer queue is not overflown and the scheduler
        // is set, then attempt to provision more slices
        if (this.slicerQueue.size() < this.queueLength) {
            await Promise.map(this.scheduler, slicerFn => slicerFn());
        }
    }

    async _executionCompleted() {
        const { logger, exId } = this;
        const { exStore } = this.stores;

        const errCount = await this._checkExecutionState();

        const executionStats = this.executionAnalytics.getAnalytics();

        if (errCount > 0) {
            const message = `execution: ${exId} had ${errCount} slice failures during processing`;
            const errorMeta = exStore.executionMetaData(executionStats, message);
            logger.error(message);
            await exStore.setStatus(exId, 'failed', errorMeta);
        } else {
            logger.info(`execution ${exId} has completed`);
            const metaData = exStore.executionMetaData(executionStats);
            await exStore.setStatus(exId, 'completed', metaData);
        }

        // FIXME: this.clusterMasterClient.executionFinished(this.exId)
    }

    _checkExecutionState() {
        const query = `ex_id:${this.exId} AND (state:error OR state:start)`;
        return this.stores.stateStore.count(query, 0);
    }

    async _waitForSlicesToComplete() {
        await Promise.delay(100);

        const availableWorkers = this.messenger.availableWorkers();
        const connectedWorkers = this.messenger.connectedWorkers();
        const remainingSlices = this.slicerQueue.size();

        this.logger.trace(`worker queue: ${availableWorkers}, connected clients ${connectedWorkers}, slicer queue: ${remainingSlices}`);

        const workersCompleted = availableWorkers >= connectedWorkers;
        const slicesFinished = remainingSlices === 0;

        if (workersCompleted && slicesFinished) {
            this.logger.info(`all work for execution: ${this.exId} has completed, starting cleanup`);
            return;
        }

        await this._waitForSlicesToComplete();
    }
}

module.exports = ExecutionController;
