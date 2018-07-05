'use strict';

const fs = require('fs-extra');
const path = require('path');
const shortid = require('shortid');
const random = require('lodash/random');
const BufferStreams = require('bufferstreams');
const archiver = require('archiver');
const { makeAssetStore } = require('../../lib/teraslice');

function zipDirectory(dir) {
    return new Promise((resolve, reject) => {
        const archive = archiver('zip', { zlib: { level: 9 } });
        archive.append(JSON.stringify({
            name: path.basename(dir),
            version: `${random(0, 100)}.${random(0, 100)}.${random(0, 100)}`,
            someProp: shortid.generate()
        }, null, 4), { name: 'asset.json' });
        archive.pipe(new BufferStreams((err, buf) => {
            if (err) {
                reject(err);
                return;
            }
            resolve(buf);
        }));
        archive.directory(dir, 'asset').finalize();
    });
}
module.exports = async (context, assetDir) => {
    const exists = await fs.pathExists(assetDir);
    if (!exists) {
        const err = new Error(`Asset Directory ${assetDir} does not exist`);
        console.error(err.stack); // eslint-disable-line no-console
        throw err;
    }
    const assetZip = await zipDirectory(assetDir);
    const assetStore = await makeAssetStore(context);
    const assetId = await assetStore.save(assetZip);
    delete context.apis.assets;
    await assetStore.shutdown(true);
    return assetId;
};
