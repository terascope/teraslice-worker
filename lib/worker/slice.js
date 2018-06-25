'use strict';

const get = require('lodash/get');
const Promise = require('bluebird');
const pRetry = require('p-retry');
const parseError = require('@terascope/error-parser');
const TerasliceWorker = require('..');

class Slice extends TerasliceWorker {
    constructor(context, jobConfig) {
        super(context, jobConfig);
        this.analytics = get(jobConfig, 'job.analytics', false);
        this._runOnce = this._runOnce.bind(this);
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
        const retries = get(this.jobConfig, 'job.max_retries', 0);

        return pRetry(this._runOnce, {
            retries,
            onFailedAttempt: (err) => {
                const errMsg = parseError(err);
                logger.error(`An error has occurred: ${errMsg}, message: `, slice);
                events.emit('slice:retry', slice);
            }
        }).then(() => {
            events.emit('slice:success', slice);
        }).catch((err) => {
            const errMsg = parseError(err);
            logger.error(`Slice failed processing: ${errMsg}, message: `, slice);
            events.emit('slice:failure', slice);
        });
    }

    _runOnce() {
        const {
            slice,
            logger,
            operations
        } = this;

        const { request } = slice;
        return Promise.reduce(operations, async (prev, fn) => {
            const data = await prev;
            return fn(data, logger, request);
        }, request);
    }
}

module.exports = Slice;
