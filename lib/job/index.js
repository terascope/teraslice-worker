'use strict';

const ExecutionRunner = require('./execution-runner');
const Job = require('./job');

module.exports = function makeJob(context, config, useExecutionRunner) {
    if (context.apis.op_runner) {
        delete context.apis.op_runner;
    }

    if (useExecutionRunner) {
        return new ExecutionRunner(context, config);
    }

    return new Job(context, config);
};
