'use strict';

const Promise = require('bluebird');
const retry = require('bluebird-retry');
const get = require('lodash/get');
const cloneDeep = require('lodash/cloneDeep');
const parseError = require('@terascope/error-parser');
const { logOpStats, WrapError } = require('./utils');
const BaseWorker = require('./base-worker');

class Slice extends BaseWorker {
    constructor(context, jobConfig) {
        super(context, jobConfig);
        this.analytics = get(jobConfig, 'job.analytics', false);
        this._runOnce = this._runOnce.bind(this);
        this._markCompleted = this._markCompleted.bind(this);
        this._markFailed = this._markFailed.bind(this);
        this._logAnalytics = this._logAnalytics.bind(this);
    }

    initialize(job, slice, stores) {
        const { slice_id: sliceId } = slice;
        const { queue } = job;
        if (this.analytics) {
            this.analyticsData = { time: [], size: [], memory: [] };
            this.operations = queue.map(fn => fn.bind(null, this.analyticsData));
        } else {
            this.operations = queue;
        }
        this.stateStore = stores.stateStore;
        this.analyticsStore = stores.analyticsStore;
        this.job = job;
        this.slice = slice;
        this.metadata = cloneDeep(get(slice, 'request'));
        this.makeLogger({ slice_id: sliceId });
    }

    start() {
        const { slice, events } = this;
        const maxRetries = get(this.jobConfig, 'job.max_retries', -1);
        const retryOptions = {
            max_tries: maxRetries,
            throw_original: true,
            interval: 100,
        };

        return retry(this._runOnce, retryOptions)
            .then(this._markCompleted)
            .catch(this._markFailed)
            .tap(this._logAnalytics)
            .finally(() => {
                events.emit('slice:finalize', slice);
            });
    }

    async _logAnalytics() {
        if (!this.analytics) return;

        const {
            logger,
            analyticsData,
            slice,
            jobConfig
        } = this;

        logOpStats(logger, slice, analyticsData);
        const executionContext = { config: jobConfig.job };
        try {
            await this.analyticsStore.log(executionContext, slice, analyticsData);
        } catch (_err) {
            throw new WrapError('Failure to update analytics', _err);
        }
    }

    async _markCompleted(result) {
        const {
            stateStore,
            slice,
            events,
            logger
        } = this;

        try {
            await stateStore.updateState(slice, 'completed');
        } catch (_err) {
            throw new WrapError('Failure to update success state', _err);
        }
        events.emit('slice:success', slice);
        logger.info('completed slice: ', slice);
        return result;
    }

    async _markFailed(err) {
        const {
            stateStore,
            slice,
            events,
            logger
        } = this;

        const errMsg = parseError(err);

        try {
            await stateStore.updateState(slice, 'error', errMsg);
        } catch (_err) {
            throw new WrapError('Failure to failure update state', _err);
        }

        events.emit('slice:failure', slice);
        logger.error(`Slice failed processing: ${errMsg}, message: `, slice);
        const sliceError = new WrapError('Slice failed processing', err);
        return Promise.reject(sliceError);
    }

    _runOnce() {
        const {
            logger,
            operations,
            events,
            slice,
            metadata,
        } = this;

        const reduceFn = (prev, fn) => Promise.resolve(prev)
            .then(data => fn(data, logger, metadata));

        return Promise.reduce(operations, reduceFn, metadata).catch((err) => {
            const errMsg = parseError(err);
            logger.error(`An error has occurred: ${errMsg}, message: `, slice);
            events.emit('slice:retry', slice);
            return Promise.reject(err);
        });
    }
}

module.exports = Slice;
