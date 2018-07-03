'use strict';

const porty = require('porty');
const random = require('lodash/random');
const ClusterMasterMessenger = require('./cluster-master-messenger');
const overrideLogger = require('./override-logger');
const saveAsset = require('./save-asset');
const TestContext = require('./test-context');
const {
    newSliceConfig,
    newJobConfig,
    newSysConfig,
    opsPath,
    newId,
} = require('./configs');

const usedPorts = [];

async function findPort() {
    const min = random(8000, 40000);
    const max = min + 100;

    const port = await porty.find({
        min,
        max,
        avoids: usedPorts,
    });

    usedPorts.push(port);

    return port;
}

module.exports = {
    findPort,
    newJobConfig,
    newSliceConfig,
    opsPath,
    newId,
    overrideLogger,
    ClusterMasterMessenger,
    saveAsset,
    newSysConfig,
    TestContext,
};
