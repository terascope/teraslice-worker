'use strict';

const Promise = require('bluebird');
const retry = require('bluebird-retry');
const get = require('lodash/get');
const cloneDeep = require('lodash/cloneDeep');
const parseError = require('@terascope/error-parser');
const { logOpStats, WrapError, makeLogger } = require('./utils');

class Slice {
    constructor(context, jobConfig) {
        this.context = context;
        this.events = context.apis.foundation.getSystemEvents();
        this.jobConfig = jobConfig;
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
        this.logger = makeLogger(this.context, this.jobConfig, { slice_id: sliceId });
    }

    run() {
        const { slice, events } = this;
        const maxRetries = get(this.jobConfig, 'job.max_retries', -1);
        const retryOptions = {
            max_tries: maxRetries,
            throw_original: true,
            interval: 100,
        };

        return Promise.resolve(this._checkSlice())
            .then(() => retry(this._runOnce, retryOptions))
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

        const errMsg = err ? parseError(err) : new Error('Unknown error occurred');

        try {
            await stateStore.updateState(slice, 'error', errMsg);
        } catch (_err) {
            throw new WrapError('Failure to update failed state', _err);
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

    async _checkSlice() {
        const { slice_id: sliceId } = this.slice;
        const { ex_id: exId } = this.jobConfig;
        const query = `ex_id:${exId} AND slice_id:${sliceId} AND (state:error OR state:completed)`;
        const count = await this.stateStore.count(query, 0);
        if (count > 0) {
            throw new Error(`Slice ${sliceId} has already been processed`);
        }
    }
}

module.exports = Slice;
