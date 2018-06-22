'use strict';

const Promise = require('bluebird');
const get = require('lodash/get');
const find = require('lodash/find');
const first = require('lodash/first');
const noop = require('lodash/noop');
const { opRunner } = require('../teraslice');

class Job {
    constructor(context, jobConfig) {
        this.context = context;
        const { job } = jobConfig;
        this.config = job;
        this.getOpConfig = this.getOpConfig.bind(this);
        this.context.apis.registerAPI('job_runner', {
            getOpConfig: this.getOpConfig,
        });
        this.opRunner = opRunner(this.context);
        if (get(this.context, 'sysconfig.teraslice.reporter')) {
            throw new Error('reporters are not functional at this time, please do not set one in the configuration');
        }
    }

    initialize() { // eslint-disable-line class-methods-use-this
        const { config } = this;
        const queue = [noop];
        return Promise.resolve({
            config,
            queue,
            reader: first(queue),
            reporter: null,
            slicer: null
        });
    }

    getOpConfig(name) {
        const operations = get(this.config, 'operations');
        return find(operations, { _op: name });
    }
}

module.exports = Job;
