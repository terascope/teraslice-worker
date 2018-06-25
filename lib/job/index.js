'use strict';

const fs = require('fs');
const path = require('path');
const Promise = require('bluebird');
const get = require('lodash/get');
const find = require('lodash/find');
const first = require('lodash/first');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const {
    opRunner: makeOpRunner,
    assetStore: makeAssetStore,
    saveAsset
} = require('../teraslice');
const { analyzeOp } = require('../utils');

const fsAccess = Promise.promisify(fs.access);

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
        await this.loadAssets();

        const queue = await Promise.map(this.operations, (opConfig, index) => {
            const op = this._loadOperation(opConfig._op);
            if (index === 0) {
                return op.newReader(context, opConfig, config);
            }
            return op.newProcessor(context, opConfig, config);
        }).map((op, index) => {
            if (!config.analytics) {
                return Promise.resolve(op);
            }
            return analyzeOp(op, index);
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
        if (isEmpty(assets)) return;

        const assetStore = await makeAssetStore(context);
        const idArray = await assetStore.parseAssetsArray(assets);
        await Promise.map(idArray, async (assetIdentifier) => {
            const downloaded = await this._alreadyDownloaded(assetIdentifier);
            // need to return the id to the assets array sent back
            if (downloaded) return { id: assetIdentifier };
            const assetRecord = await assetStore.get(assetIdentifier);
            logger.info(`loading assets: ${assetIdentifier}`);
            const buff = Buffer.from(assetRecord.blob, 'base64');
            return saveAsset(logger, assetsDirectory, assetIdentifier, buff);
        });
        await assetStore.shutdown();
    }

    async _alreadyDownloaded(assetIdentifier) {
        const { assetsDirectory } = this;
        try {
            await fsAccess(path.join(assetsDirectory, assetIdentifier));
            return true;
        } catch (err) {
            return false;
        }
    }

    _loadOperation(opName) {
        const { assets, assetsDirectory, opRunner } = this;

        const assetPath = !isEmpty(assets) ? assetsDirectory : null;
        if (!isString(opName)) {
            throw new Error('please verify that ops_directory in config and _op for each job operations are strings');
        }

        const codePath = opRunner.findOp(opName, assetPath, assets);
        try {
            return require(codePath);
        } catch (error) {
            // if it cant be required check first error to see if it exists
            // or had an error loading
            if (error.message !== 'missing path') {
                error.message = `Failed to module: ${opName}, the following error occurred while attempting to load the code: ${error.message}`;
            }
            try {
                return require(opName);
            } catch (err) {
                err.message = new Error(`Error loading module: ${opName}, for reasons ${err.stack} & ${error.stack}`);

                if (err.code && err.code === 'MODULE_NOT_FOUND') {
                    err.message = `Could not retrieve code for: ${opName}, error message: ${err.message}`;
                }
                throw err;
            }
        }
    }
}

module.exports = Job;
