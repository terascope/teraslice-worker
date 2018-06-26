'use strict';

const Promise = require('bluebird');
const retry = require('bluebird-retry');
const get = require('lodash/get');
const cloneDeep = require('lodash/cloneDeep');
const NestedError = require('nested-error-stacks');
const parseError = require('@terascope/error-parser');
const TerasliceWorker = require('..');

class Slice extends TerasliceWorker {
    constructor(context, jobConfig) {
        super(context, jobConfig);
        this.analytics = get(jobConfig, 'job.analytics', false);
    }

    initialize(job, slice) {
        const { sliceId } = slice;
        const { queue } = job;
        if (this.analytics) {
            this.specData = { time: [], size: [], memory: [] };
            this.operations = queue.map(fn => fn.bind(null, this.specData));
        } else {
            this.operations = queue;
        }
        this.job = job;
        this.slice = slice;
        this.makeLogger({ slice_id: sliceId });
    }

    start() {
        const { logger, slice, events } = this;
        const maxRetries = get(this.jobConfig, 'job.max_retries', -1);

        return retry(() => {
            const msg = cloneDeep(get(slice, 'request'));
            return this._runOnce(msg).catch((err) => {
                const errMsg = parseError(err);
                logger.error(`An error has occurred: ${errMsg}, message: `, slice);
                events.emit('slice:retry', slice);
                return Promise.reject(err);
            });
        }, {
            max_tries: maxRetries,
            context: this,
            throw_original: true,
            interval: 100,
        }).then((results) => {
            events.emit('slice:success', slice);
            return results;
        }).catch((err) => {
            const error = new NestedError(`Slice failed processing: ${err.toString()}`, err);
            const errMsg = parseError(err);
            logger.error(`Slice failed processing: ${errMsg}, message: `, slice);
            events.emit('slice:failure', slice);
            return Promise.reject(error);
        });
    }

    _runOnce(msg) {
        const {
            logger,
            operations,
        } = this;

        return Promise.reduce(operations, (prev, fn) => {
            const p = Promise.resolve(prev)
                .then(data => fn(data, logger, msg));
            return p;
        }, msg);
    }
}

module.exports = Slice;
