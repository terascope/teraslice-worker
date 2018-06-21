'use strict';

const {
    isPlainObject,
    isEmpty,
    isString,
    isNumber
} = require('lodash');
const shortid = require('shortid');
const { makeContext } = require('./terafoundation');
const { getTerasliceConfig } = require('./teraslice');

// const Worker = require('./worker')
const workerTypes = {
    'worker': WorkerAssignment
}

class TerasliceWorker {
    constructor(config, jobConfig) {
        validateJobConfig(jobConfig);
        this.context = generateContext(config);
        this.jobConfig = jobConfig;
        this._init();
    }

    async start() { // eslint-disable-line class-methods-use-this

    }

    async shutdown() { // eslint-disable-line class-methods-use-this

    }

    instance() {
        return this._instance;
    }

    _init() {
        const { exId, jobId, assignment } = this.jobConfig;
        const { hostname } = this.context.sysconfig.teraslice;
        const { makeLogger, getSystemEvents } = this.context.apis.foundation;
        this.workerId = `${hostname}.${shortid.generate()}`;
        this.logger = makeLogger({
            ex_id: exId,
            job_id: jobId,
            module: `teraslice-worker:${assignment}`,
            worker_id: this.workerId,
        });
        this.events = getSystemEvents();
    }
}

function validateJobConfig(config) {
    if (!isPlainObject(config) || isEmpty(config)) {
        throw new Error('Worker requires a valid job configuration');
    }

    const {
        assignment,
        job,
        exId,
        jobId,
        slicerPort
    } = config;

    if (!isString(assignment)) {
        throw new Error('Job configuration requires a valid assignment');
    }
    if (!isPlainObject(job) || isEmpty(job)) {
        throw new Error('Job configuration requires a valid job');
    }
    if (!isString(exId)) {
        throw new Error('Job configuration requires a valid exId');
    }
    if (!isString(jobId)) {
        throw new Error('Job configuration requires a valid jobId');
    }
    if (!isNumber(slicerPort)) {
        throw new Error('Job configuration requires a valid slicerPort');
    }

    if (!has(availableAssignments, assignment))
}

function generateContext(sysconfig) {
    if (!isPlainObject(sysconfig) || isEmpty(sysconfig)) {
        throw new Error('Worker requires a valid terafoundation configuration');
    }

    const config = getTerasliceConfig({ name: 'teraslice-worker' });
    return makeContext(config, sysconfig);
}

module.exports = TerasliceWorker;
module.exports.validateJobConfig = validateJobConfig;
module.exports.generateContext = generateContext;
