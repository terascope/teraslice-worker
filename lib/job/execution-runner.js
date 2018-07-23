'use strict';

const _ = require('lodash');
const makeExecutionRunner = require('teraslice/lib/cluster/runners/execution');
const { WrapError, validateConfig } = require('../utils');
const Assets = require('./assets');

class ExecutionRunner {
    constructor(context, config) {
        validateConfig(config);
        this.runner = makeExecutionRunner(context, {
            execution: config.job,
            processAssingment: config
        });

        this.assignment = config.assignment;

        this.assets = new Assets(context, config);
        this.api = {};
        this.api.config = config.job;
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

        if (this.assignment === 'execution_controller') {
            await this._setQueueLength();
        }

        return this.api;
    }

    async shutdown() {
        await this.assets.shutdown();
        this.api = null;
    }

    async _setQueueLength() {
        const { slicer } = this.api;

        const defaultLength = 10000;
        this.api.queueLength = defaultLength;
        this.api.dynamicQueueLength = false;

        if (!slicer.slicerQueueLength) return;
        if (!_.isFunction(slicer.slicerQueueLength)) {
            this.logger.error(`slicerQueueLength on the reader must be a function, defaulting to ${defaultLength}`);
            return;
        }

        const results = await slicer.slicerQueueLength(this.api);

        if (results === 'QUEUE_MINIMUM_SIZE') {
            this.api.dynamicQueueLength = true;
            this.api.queueLength = this.api.config.workers;
        } else if (_.isNumber(results) && results >= 1) {
            this.api.queueLength = results;
        }

        const isDyanmic = this.api.dynamicQueueLength ? ' and is dynamic' : '';

        this.logger.info(`Setting slicer queue length to ${this.api.queueLength}${isDyanmic}`);
    }
}

module.exports = ExecutionRunner;
