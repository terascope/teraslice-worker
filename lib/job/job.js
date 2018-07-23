'use strict';

const _ = require('lodash');
const Assets = require('./assets');
const {
    analyzeOp,
    WrapError,
    validateConfig,
    makeLogger,
} = require('../utils');
const { makeOpRunner } = require('../teraslice');

class Job {
    constructor(context, config) {
        if (_.get(context, 'sysconfig.teraslice.reporter')) {
            throw new Error('reporters are not functional at this time, please do not set one in the configuration');
        }

        validateConfig(config);

        this.getOpConfig = this.getOpConfig.bind(this);
        this._loadOperation = this._loadOperation.bind(this);

        if (context.apis.op_runner) {
            delete context.apis.op_runner;
        }

        context.apis.registerAPI('job_runner', {
            getOpConfig: this.getOpConfig,
        });

        this.opRunner = makeOpRunner(context, { skipRegister: true });

        this.context = context;
        this.logger = makeLogger(context, config, 'job');

        this.operations = _.get(config.job, 'operations', []);
        this.assignment = config.assignment;

        this.assets = new Assets(context, config);

        this.api = {};
        this.api.config = config.job;
        this.api.queue = [];
        this.api.reader = null;
        this.api.slicer = null;
        this.api.reporter = null;
    }

    getOpConfig(name) {
        return _.find(this.operations, { _op: name });
    }

    async initialize() {
        await this.assets.load();

        if (this.assignment === 'worker') {
            await this._initializeOperations();
        }
        if (this.assignment === 'execution_controller') {
            await this._initializeSlicer();
        }
        return this.api;
    }

    async shutdown() {
        await this.assets.shutdown();
        this.api = null;
    }

    async _initializeSlicer() {
        const opConfig = _.first(this.operations);
        this.api.slicer = await this._loadOperation(opConfig._op);
        await this._setQueueLength();
    }

    async _initializeOperations() {
        const { context } = this;
        const { config } = this.api;

        this.api.queue = await Promise.map(this.operations, async (opConfig, index) => {
            const op = await this._loadOperation(opConfig._op);
            const args = [context, opConfig, config];
            const opFn = !index ? await op.newReader(...args) : await op.newProcessor(...args);
            if (!config.analytics) {
                return opFn;
            }
            return analyzeOp(opFn, index);
        });

        this.api.reader = _.first(this.api.queue);
    }

    async _loadOperation(opName) {
        const { findOp } = this.opRunner;
        const { assetIds, assetsDirectory } = this.assets;

        const assetPath = !_.isEmpty(assetIds) ? assetsDirectory : null;
        if (!_.isString(opName)) {
            throw new WrapError('please verify that ops_directory in config and _op for each job operations are strings');
        }

        const codePath = findOp(opName, assetPath, assetIds);
        try {
            return require(codePath);
        } catch (_error) {
            const error = new WrapError(`Failed to module by path: ${opName}`, _error);
            try {
                return require(opName);
            } catch (err) {
                if (_.get(err, 'code') === 'MODULE_NOT_FOUND') {
                    err.message = `Could not retrieve code for: ${opName}`;
                }
                const wrappedError = new WrapError(error.toString(), err);
                throw new WrapError(`Failed to module: ${opName}`, wrappedError);
            }
        }
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

module.exports = Job;
