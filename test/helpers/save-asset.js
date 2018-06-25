'use strict';

const Promise = require('bluebird');
const BufferStreams = require('bufferstreams');
const archiver = require('archiver');
const { assetStore: makeAssetStore } = require('../../lib/teraslice');

function zipDirectory(dir) {
    return new Promise((resolve, reject) => {
        const archive = archiver('zip', { zlib: { level: 9 } });
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
    const assetZip = await zipDirectory(assetDir);
    const assetStore = await makeAssetStore(context);
    const assetId = await assetStore.save(assetZip);
    delete context.apis.assets;
    await assetStore.shutdown();
    return assetId;
};
