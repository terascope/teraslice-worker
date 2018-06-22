'use strict';

const Promise = require('bluebird');
const get = require('lodash/get');
const find = require('lodash/find');
const first = require('lodash/first');
const isEmpty = require('lodash/isEmpty');
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
        this.loadOp = opRunner(this.context).load;

        if (get(this.context, 'sysconfig.teraslice.reporter')) {
            throw new Error('reporters are not functional at this time, please do not set one in the configuration');
        }
    }

    initialize() { // eslint-disable-line class-methods-use-this
        const { config, context, loadOp } = this;
        const assets = get(config, 'assets', []);
        const assetsDirectory = get(context, 'sysconfig.teraslice.assets_directory');
        const assetPath = !isEmpty(assets) ? assetsDirectory : null;
        const operations = get(config, 'operations', []);
        return Promise.map(operations, (opConfig, index) => {
            const op = loadOp(opConfig._op, assetPath, assets);
            if (index === 0) {
                return op.newReader(context, opConfig, config);
            }
            return op.newProcessor(context, opConfig, config);
        }).then((queue) => {
            const reader = first(queue);
            return {
                config,
                queue,
                reader,
                reporter: null,
                slicer: null
            };
        });
    }

    getOpConfig(name) {
        const operations = get(this.config, 'operations');
        return find(operations, { _op: name });
    }
}

module.exports = Job;
