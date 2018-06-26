'use strict';

const fs = require('fs-extra');
const path = require('path');
const Promise = require('bluebird');
const get = require('lodash/get');
const find = require('lodash/find');
const first = require('lodash/first');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const NestedError = require('nested-error-stacks');
const {
    opRunner: makeOpRunner,
    assetStore: makeAssetStore,
    saveAsset
} = require('../teraslice');
const { analyzeOp } = require('../utils');

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
    }

    getOpConfig(name) {
        return find(this.operations, { _op: name });
    }

    async initialize() {
        const { config, context } = this;
        this.assetIds = await this.loadAssets();

        const queue = await Promise.map(this.operations, async (opConfig, index) => {
            const op = await this._loadOperation(opConfig._op);
            const args = [context, opConfig, config];
            const opFn = !index ? await op.newReader(...args) : await op.newProcessor(...args);
            if (!config.analytics) {
                return opFn;
            }
            return analyzeOp(opFn, index);
        });

        const reader = first(queue);
        return {
            config,
            queue,
            reader,
            reporter: null,
            slicer: null
        };
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
            if (isString(err)) {
                throw new Error(err);
            }
            throw err;
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

        await assetStore.shutdown();
        return idArray;
    }

    async _loadOperation(opName) {
        const { assetIds, opRunner } = this;

        const assetPath = !isEmpty(assetIds) ? this.assetsDirectory : null;
        if (!isString(opName)) {
            throw new Error('please verify that ops_directory in config and _op for each job operations are strings');
        }

        const codePath = opRunner.findOp(opName, assetPath, assetIds);
        let finalError;
        try {
            return require(codePath);
        } catch (error) {
            finalError = error;
            // if it cant be required check first error to see if it exists
            // or had an error loading
            if (error.message !== 'missing path') {
                finalError = new NestedError(`Failed to module: ${opName}, the following error occurred while attempting to load the code`, error);
            }
            try {
                return require(opName);
            } catch (err) {
                if (err.code && err.code === 'MODULE_NOT_FOUND') {
                    finalError = new NestedError(`Could not retrieve code for: ${opName}`, err);
                } else {
                    finalError = new NestedError(`Error loading module: ${opName}`, err);
                }

                throw finalError;
            }
        }
    }
}

module.exports = Job;
