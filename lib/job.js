'use strict';

const fs = require('fs-extra');
const path = require('path');
const Promise = require('bluebird');
const get = require('lodash/get');
const find = require('lodash/find');
const first = require('lodash/first');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const { analyzeOp, WrapError } = require('./utils');
const {
    opRunner: makeOpRunner,
    assetStore: makeAssetStore,
    saveAsset
} = require('./teraslice');

class Job {
    constructor(context, jobConfig) {
        if (get(context, 'sysconfig.teraslice.reporter')) {
            throw new Error('reporters are not functional at this time, please do not set one in the configuration');
        }
        this.context = context;
        this.config = jobConfig.job;
        this.getOpConfig = this.getOpConfig.bind(this);
        this._loadOperation = this._loadOperation.bind(this);
        this.context.apis.registerAPI('job_runner', {
            getOpConfig: this.getOpConfig,
        });
        this.opRunner = makeOpRunner(this.context);
        this.assets = get(this.config, 'assets', []);
        this.assetsDirectory = get(context, 'sysconfig.teraslice.assets_directory');
        this.operations = get(this.config, 'operations', []);

        this.queue = [];
        this.reader = null;
        this.executionController = null;
        this.reporter = null;
    }

    getOpConfig(name) {
        return find(this.operations, { _op: name });
    }

    async initialize() {
        const { config, context } = this;
        this.assetIds = await this.loadAssets();

        this.queue = await Promise.map(this.operations, async (opConfig, index) => {
            const op = await this._loadOperation(opConfig._op);
            const args = [context, opConfig, config];
            const opFn = !index ? await op.newReader(...args) : await op.newProcessor(...args);
            if (!config.analytics) {
                return opFn;
            }
            return analyzeOp(opFn, index);
        });

        this.reader = first(this.queue);
        this.reporter = null;
        this.executionController = null;
    }

    async loadAssets() {
        const {
            context,
            assets,
            assetsDirectory
        } = this;
        const { logger } = context;

        // no need to load assets
        if (isEmpty(assets)) return [];

        const assetStore = await makeAssetStore(context);
        let idArray;

        try {
            idArray = await assetStore.parseAssetsArray(assets);
        } catch (err) {
            throw new WrapError(err);
        }

        await Promise.map(idArray, async (assetIdentifier) => {
            const downloaded = await fs.pathExists(path.join(assetsDirectory, assetIdentifier));
            // need to return the id to the assets array sent back
            if (downloaded) return { id: assetIdentifier };

            const assetRecord = await assetStore.get(assetIdentifier);
            logger.info(`loading assets: ${assetIdentifier}`);
            const buff = Buffer.from(assetRecord.blob, 'base64');
            return saveAsset(logger, assetsDirectory, assetIdentifier, buff);
        });

        await assetStore.destroy();
        return idArray;
    }

    async _loadOperation(opName) {
        const { assetIds, opRunner } = this;

        const assetPath = !isEmpty(assetIds) ? this.assetsDirectory : null;
        if (!isString(opName)) {
            throw new WrapError('please verify that ops_directory in config and _op for each job operations are strings');
        }

        const codePath = opRunner.findOp(opName, assetPath, assetIds);
        try {
            return require(codePath);
        } catch (_error) {
            const error = new WrapError(`Failed to module by path: ${opName}`, _error);
            try {
                return require(opName);
            } catch (err) {
                if (get(err, 'code') === 'MODULE_NOT_FOUND') {
                    err.message = `Could not retrieve code for: ${opName}`;
                }
                const wrappedError = new WrapError(error.toString(), err);
                throw new WrapError(`Failed to module: ${opName}`, wrappedError);
            }
        }
    }
}

module.exports = Job;
