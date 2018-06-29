'use strict';

const path = require('path');
const shortid = require('shortid');
const random = require('lodash/random');
const ClusterMasterMessenger = require('./cluster-master-messenger');
const overrideLogger = require('./override-logger');
const saveAsset = require('./save-asset');
const TestContext = require('./test-context');
const terasliceConfig = require('./teraslice-config');

const newId = prefix => `${prefix}-${shortid.generate()}`.toLowerCase();
const opsPath = path.join(__dirname, '..', 'fixtures', 'ops');

const newSliceConfig = (request = { example: 'slice-data' }) => ({
    slice_id: newId('slice-id'),
    slicer_id: newId('slicer-id'),
    order: random(0, 1000),
    request,
    _created: new Date().toISOString()
});

module.exports = {
    newSliceConfig,
    opsPath,
    newId,
    overrideLogger,
    ClusterMasterMessenger,
    saveAsset,
    terasliceConfig,
    TestContext,
};
