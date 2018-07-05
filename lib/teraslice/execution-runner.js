'use strict';

const makeExecutionRunner = require('teraslice/lib/cluster/runners/execution');
const { WrapError, validateJobConfig } = require('../utils');
const Assets = require('../assets');

class ExecutionRunner {
    constructor(context, jobConfig) {
        validateJobConfig(jobConfig);
        this.runner = makeExecutionRunner(context, {
            execution: jobConfig.job,
            processAssingment: jobConfig
        });
        this.assets = new Assets(context, jobConfig);
        this.api = {};
        this.api.config = jobConfig.job;
        this.api.queue = [];
        this.api.reader = null;
        this.api.slicer = null;
        this.api.reporter = null;
    }

    async initialize() {
        await this.assets.load();
        try {
            this.api = await this.runner.initialize();
        } catch (err) {
            throw new WrapError('Unable to initialize runner', err);
        }
        return this.api;
    }
}

module.exports = ExecutionRunner;
