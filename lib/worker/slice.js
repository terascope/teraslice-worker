'use strict';

const get = require('lodash/get');
const Promise = require('bluebird');
const TerasliceWorker = require('..');

class Slice extends TerasliceWorker {
    constructor(context, jobConfig) {
        super(context, jobConfig);
        this.analytics = get(jobConfig, 'job.analytics', false);
    }

    initialize(job, sliceConfig) {
        const { sliceId } = sliceConfig;
        const { queue } = job;
        if (this.analytics) {
            this.specData = { time: [], size: [], memory: [] };
            this.operations = queue.map(fn => fn.bind(null, this.specData));
        } else {
            this.operations = queue;
        }
        this.job = job;
        this.sliceConfig = sliceConfig;
        this.makeLogger({ slice_id: sliceId });
    }

    start() {
        const {
            sliceConfig,
            logger,
            operations
        } = this;

        const { request } = sliceConfig;
        return Promise.reduce(operations, async (prev, fn) => {
            const data = await prev;
            return fn(data, logger, request);
        }, request);
    }
}

module.exports = Slice;
