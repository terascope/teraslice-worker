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
        this.stores = stores;
        this.job = job;
        this.slice = slice;
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

        return retry(this._runOnce, retryOptions)
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
            slice
        } = this;

        const msg = cloneDeep(get(slice, 'request'));
        const reduceFn = (prev, fn) => Promise.resolve(prev)
            .then(data => fn(data, logger, msg));
        return Promise.reduce(operations, reduceFn, msg).catch((err) => {
            const errMsg = parseError(err);
            logger.error(`An error has occurred: ${errMsg}, message: `, slice);
            events.emit('slice:retry', slice);
            return Promise.reject(err);
        });
    }
}

module.exports = Slice;
