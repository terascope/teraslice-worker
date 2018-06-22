'use strict';

const shortid = require('shortid');
const terasliceConfig = require('./teraslice-config');
const { overrideLoggerOnContext } = require('./override-logger');
const { generateContext } = require('../../lib/utils');

module.exports = (testName) => {
    const clusterName = `tmp_${shortid.generate()}`.toLowerCase();
    const config = terasliceConfig({ clusterName });
    const context = generateContext(config);
    overrideLoggerOnContext(context, testName);
    return {
        context,
        _context: context,
        config,
        _config: config,
        clusterName,
        _clusterName: clusterName,
    };
};
