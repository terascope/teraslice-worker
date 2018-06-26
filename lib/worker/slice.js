'use strict';

const Promise = require('bluebird');
const retry = require('bluebird-retry');
const get = require('lodash/get');
const cloneDeep = require('lodash/cloneDeep');
const TerasliceError = require('../utils/error');
const parseError = require('@terascope/error-parser');
const TerasliceWorker = require('..');

class Slice extends TerasliceWorker {
    constructor(context, jobConfig) {
        super(context, jobConfig);
        this.analytics = get(jobConfig, 'job.analytics', false);
        this._runOnce = this._runOnce.bind(this);
    }

    initialize(job, slice, stores) {
        const { sliceId } = slice;
        const { queue } = job;
        if (this.analytics) {
            this.specData = { time: [], size: [], memory: [] };
            this.operations = queue.map(fn => fn.bind(null, this.specData));
        } else {
            this.operations = queue;
        }
        this.stateStore = stores.stateStore;
        this.job = job;
        this.slice = slice;
        this.metadata = cloneDeep(get(slice, 'request'));
        this.makeLogger({ slice_id: sliceId });
    }

    start() {
        const { logger, slice, events } = this;
        const maxRetries = get(this.jobConfig, 'job.max_retries', -1);
        const retryOptions = {
            max_tries: maxRetries,
            throw_original: true,
            interval: 100,
        };

        retry(this._runOnce, retryOptions)
            .tap(() => this._markCompleted())
            .then((results) => {
                events.emit('slice:success', slice);
                return results;
            })
            .catch((err) => {
                const error = new TerasliceError(`Slice failed processing: ${err.toString()}`, err);
                const errMsg = parseError(err);
                logger.error(`Slice failed processing: ${errMsg}, message: `, slice);
                events.emit('slice:failure', slice);
                return Promise.reject(error);
            })
            .then((results) => {
                events.emit('slice:finalize', slice);
                return results;
            })
            .catch((error) => {
                events.emit('slice:finalize', slice);
                return Promise.reject(error);
            });
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

    async _markCompleted() {
        const { stateStore, slice } = this;
        try {
            await stateStore.updateState(slice, 'completed');
        } catch (_err) {
            throw new TerasliceError('Failure to update state', _err);
        }
    }
}

module.exports = Slice;
