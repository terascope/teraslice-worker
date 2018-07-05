'use strict';

require('./global-setup');

const Worker = require('./lib/worker');
const ExecutionController = require('./lib/execution-controller');

module.exports = {
    Worker,
    ExecutionController
};
