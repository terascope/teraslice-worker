'use strict';

const Promise = require('bluebird');
const get = require('lodash/get');
const castArray = require('lodash/castArray');
const uuidv4 = require('uuid/v4');
const retry = require('bluebird-retry');
const Queue = require('@terascope/queue');
const parseError = require('@terascope/error-parser');
const ClusterMasterClient = require('./cluster-master-client');
const ExecutionAnalytics = require('./analytics');
const ExecutionControllerMessenger = require('./messenger');
const {
    makeStateStore,
    makeExStore,
    makeSliceAnalytics,
    newFormatedDate
} = require('../teraslice');
const makeEngine = require('../teraslice/engine');
const {
    generateWorkerId,
    makeLogger,
    WrapError,
    formatURL,
} = require('../utils');

const immediate = Promise.promisify(setImmediate);

/**
 * Teraslice Worker, assignment type "execution_controller"
 * @export
 * @class ExecutionController
 * @param {object} context             terafoundation context
 * @param {object} executionContext    teraslice execution context
 */
class ExecutionController {
    constructor(context, executionContext) {
        const workerId = generateWorkerId(context);
        const logger = makeLogger(context, executionContext, 'execution_controller');
        const events = context.apis.foundation.getSystemEvents();

        const slicerPort = executionContext.slicer_port;
        const networkerLatencyBuffer = get(context, 'sysconfig.teraslice.network_latency_buffer');
        const actionTimeout = get(context, 'sysconfig.teraslice.action_timeout');

        this.messenger = new ExecutionControllerMessenger({
            port: slicerPort,
            networkerLatencyBuffer,
            actionTimeout,
            events,
        });

        const clusterMasterPort = get(context, 'sysconfig.teraslice.port');
        const clusterMasterHostname = get(context, 'sysconfig.teraslice.master_hostname');

        this.clusterMasterClient = new ClusterMasterClient({
            clusterMasterUrl: formatURL(clusterMasterHostname, clusterMasterPort),
            nodeId: executionContext.node_id,
            networkerLatencyBuffer,
            actionTimeout,
        });

        this.executionAnalytics = new ExecutionAnalytics(context, executionContext, this.messenger);

        this.slicerQueue = new Queue();

        this.workerId = workerId;
        this.logger = logger;
        this.events = events;
        this.context = context;
        this.executionContext = executionContext;
        this.collectAnalytics = this.executionContext.config.analytics;

        this.stores = {};
        this.slicerDone = 0;
        this.isRecovery = false;
        this.isShuttingDown = false;
        this.isInitialized = false;

        this._dispatchSlices = this._dispatchSlices.bind(this);
        this.setFailingStatus = this.setFailingStatus.bind(this);
    }

    async initialize() {
        const { context } = this;
        const { ex_id: exId } = this.executionContext;

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
            this.executionAnalytics.increment('workers_disconnected');
            this._adjustSlicerQueueLength();
        });

        this.messenger.on('worker:reconnect', (response) => {
            this.executionAnalytics.increment('workers_reconnected');

            this.logger.warn(`worker: ${response.worker_id} has rejoined slicer: ${exId}`);
        });

        this.events.on('slice:success', (response) => {
            this.executionAnalytics.increment('processed');

            if (this.collectAnalytics) {
                this.slicerAnalytics.addStats(response.analytics);
            }

            this.logger.debug(`worker ${response.worker_id} has completed its slice,`, response);
        });

        this.events.on('slice:failure', (response) => {
            this.executionAnalytics.increment('processed');
            this.executionAnalytics.increment('failed');

            this.logger.error(`worker: ${response.worker_id} has error on slice: ${JSON.stringify(response)} , slicer: ${exId}`);
        });

        this.events.on('slicer:execution:update', ({ update }) => {
            this.logger.debug('slicer sending a execution update', update);

            // this is updating the opConfig for elasticsearch start and/or end dates for ex,
            // this assumes elasticsearch is first
            this.stores.exStore.update(exId, { operations: update });
        });

        this.engine = makeEngine(this);

        await this._slicerInit();

        this.isInitialized = true;
    }

    async _slicerInit() {
        const { logger, context, executionContext } = this;

        const maxRetries = get(executionContext, 'config.max_retries', 3);
        const retryOptions = {
            max_tries: maxRetries,
            throw_original: true,
            interval: 100,
        };

        this.slicers = await retry(() => {
            const startingPointStateRecords = undefined; // FIXME: when recovery is implemented
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
            context,
            executionContext
        } = this;
        const {
            ex_id: exId,
            job_id: jobId
        } = executionContext;

        const { exStore } = this.stores;

        this.startTime = Date.now();

        this.slicerAnalytics = makeSliceAnalytics(context, executionContext, {
            exId,
            jobId,
        });

        await this.executionAnalytics.start();

        this.scheduler = this.engine.registerSlicers(this.slicers, this.isRecovery);

        await exStore.setStatus(exId, 'running');

        return Promise.all([
            this._processSlices(),
            this._doneProcessing(),
        ]);
    }

    async allocateSlice(request, slicerId, startingOrder) {
        let slicerOrder = startingOrder;
        const { logger, slicerQueue } = this;
        const { stateStore } = this.stores;
        const { ex_id: exId } = this.executionContext;

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
        const { ex_id: exId } = this.executionContext;
        const { exStore } = this.stores;

        const errMsg = `slicer: ${exId} has encountered a processing_error`;
        this.logger.error(errMsg);

        const executionStats = this.executionAnalytics.getAnalytics();
        const errorMeta = await exStore.executionMetaData(executionStats, errMsg);
        await exStore.setStatus(exId, 'failing', errorMeta);
    }

    async slicerFailure(err) {
        const { ex_id: exId } = this.executionContext;
        const { exStore } = this.stores;

        this.stopProcessing = true;

        const error = new WrapError(`slicer for ex ${exId} had an error, shutting down execution`, err);
        this.logger.error(error.toString());

        const executionStats = this.executionAnalytics.getAnalytics();
        const errorMeta = await exStore.executionMetaData(executionStats, error.toString());

        await exStore.setStatus(exId, 'failed', errorMeta);

        await this.messenger.executionTerminal(exId);
    }

    async slicerCompleted() {
        const { ex_id: exId } = this;

        this.slicerDone += 1;

        this.logger.info(`a slicer for execution: ${exId} has completed its range`);

        if (!this._slicersDone()) return;

        this.events.emit('slicers:finished');

        this.logger.info(`all slicers for execution: ${exId} have been completed, waiting for slices in slicerQueue to be processed`);
        this.executionAnalytics.set('queuing_complete', newFormatedDate());
    }

    isDone() {
        return this.isShuttingDown || this.stopProcessing || this._slicersDone();
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
            await this.executionAnalytics.shutdown();
        } catch (err) {
            shutdownErrs.push(err);
        }

        try {
            await this.clusterMasterClient.shutdown();
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

    _slicersDone() {
        return this.slicerDone === this.slicers.length;
    }

    async _adjustSlicerQueueLength() {
        const { dynamicQueueLength, queueLength } = this.executionContext;
        if (!dynamicQueueLength) return;

        const clientsOnline = this.messenger.connectedWorkers();

        if (clientsOnline > queueLength) {
            this.executionContext.queueLength = clientsOnline;
            this.logger.info(`adjusted queue length ${this.executionContext.queueLength}`);
        }
    }

    _logFinishedJob() {
        const { ex_id: exId } = this.executionContext;

        const endTime = Date.now();
        const time = (endTime - this.startTime) / 1000;

        this.executionAnalytics.set('job_duration', time);

        if (this.collectAnalytics) {
            this.slicerAnalytics.analyzeStats();
        }

        this.logger.info(`execution ${exId} has finished in ${time} seconds`);
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

        await immediate();

        await this._processSlices();
    }

    async _dispatchSlices() {
        if (this.isDone()) return;
        if (!this.slicerQueue.size()) return;

        if (!this.messenger.availableWorkers()) {
            const foundWorker = await this.messenger.onceWithTimeout('worker:enqueue', null, true);
            if (!foundWorker) return;
        }

        const slice = this.slicerQueue.dequeue();

        const { dispatched, workerId } = await this.messenger.dispatchSlice(slice);

        if (dispatched) {
            this.logger.debug(`dispatched slice ${slice.slice_id} to worker ${workerId}`);
        } else {
            this.slicerQueue.unshift(slice);
            this.logger.debug(`worker ${workerId} is not available to process slice ${slice.slice_id}`);
        }

        await immediate();

        await this._dispatchSlices();
    }

    async _createSlices() {
        if (!this.scheduler) return;
        if (this._slicersDone()) return;

        // If all slicers are not done, the slicer queue is not overflown and the scheduler
        // is set, then attempt to provision more slices
        if (this.slicerQueue.size() < this.executionContext.queueLength) {
            await Promise.map(this.scheduler, slicerFn => slicerFn());
        }
    }

    async _doneProcessing() {
        const { events } = this;
        const { ex_id: exId } = this.executionContext;

        await new Promise((resolve) => {
            const id = setInterval(() => {
                if (this.isDone() && !this.slicerQueue.size()) {
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
            this.logger.error(`execution ${exId} has run to completion but the process has failed while updating the execution status, slicer will soon exit, error: ${errMsg}`);
        }

        await this._logFinishedJob();
    }

    async _waitForSlicesToComplete() {
        const { ex_id: exId } = this.executionContext;

        await Promise.delay(100);

        const availableWorkers = this.messenger.availableWorkers();
        const connectedWorkers = await this.messenger.connectedWorkers();
        const remainingSlices = this.slicerQueue.size();

        this.logger.trace(`worker queue: ${availableWorkers}, connected clients ${connectedWorkers}, slicer queue: ${remainingSlices}`);

        const workersCompleted = availableWorkers >= connectedWorkers;
        const slicesFinished = remainingSlices === 0;

        if (workersCompleted && slicesFinished) {
            this.logger.info(`all work for execution: ${exId} has completed, starting cleanup`);
            return;
        }

        await this._waitForSlicesToComplete();
    }

    async _executionCompleted() {
        const { logger } = this;
        const { exStore } = this.stores;
        const { ex_id: exId } = this.executionContext;

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

        await this.messenger.executionFinished(exId);
    }

    _checkExecutionState() {
        const { ex_id: exId } = this.executionContext;

        const query = `ex_id:${exId} AND (state:error OR state:start)`;
        return this.stores.stateStore.count(query, 0);
    }
}

module.exports = ExecutionController;
