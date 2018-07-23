'use strict';

const fs = require('fs-extra');
const path = require('path');
const isEmpty = require('lodash/isEmpty');
const get = require('lodash/get');
const { WrapError, validateConfig } = require('./utils');
const {
    makeAssetStore,
    saveAsset,
} = require('./teraslice');

class Assets {
    constructor(context, config) {
        validateConfig(config);

        this.context = context;
        this.config = config;
        this.assets = get(config.job, 'assets', []);
        this.assetsDirectory = get(context, 'sysconfig.teraslice.assets_directory');
        this.assetIds = [];
    }

    async load() {
        const {
            context,
            assets,
            assetsDirectory
        } = this;
        const { logger } = context;

        // no need to load assets
        if (isEmpty(assets)) return;

        this.assetStore = await makeAssetStore(context);
        let idArray;

        try {
            idArray = await this.assetStore.parseAssetsArray(assets);
        } catch (err) {
            throw new WrapError(err);
        }

        await Promise.map(idArray, async (assetIdentifier) => {
            const downloaded = await fs.pathExists(path.join(assetsDirectory, assetIdentifier));
            // need to return the id to the assets array sent back
            if (downloaded) return { id: assetIdentifier };

            const assetRecord = await this.assetStore.get(assetIdentifier);
            logger.info(`loading assets: ${assetIdentifier}`);
            const buff = Buffer.from(assetRecord.blob, 'base64');
            return saveAsset(logger, assetsDirectory, assetIdentifier, buff);
        });

        this.assetIds = idArray;
    }

    async shutdown() {
        if (this.assetStore) {
            await this.assetStore.shutdown(true);
        }
        this.assetStore = null;
        this.assetIds = [];
    }
}

module.exports = Assets;
