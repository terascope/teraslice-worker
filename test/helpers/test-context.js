'use strict';

const shortid = require('shortid');
const terasliceConfig = require('./teraslice-config');
const { generateContext } = require('../../lib/utils');

module.exports = () => {
    const clusterName = `tmp_${shortid.generate()}`.toLowerCase();
    const config = terasliceConfig({ clusterName });
    const context = generateContext(config);
    return {
        context,
        _context: config,
        config,
        _config: config,
        clusterName,
        _clusterName: clusterName,
    };
};
