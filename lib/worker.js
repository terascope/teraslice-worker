'use strict';

const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const isNumber = require('lodash/isNumber');
const shortid = require('shortid');

const { makeContext } = require('./terafoundation');
const { getTerasliceConfig } = require('./teraslice');

class Worker {
    constructor(config, jobConfig) {
        validateJobConfig(jobConfig);
        this.context = generateContext(config);
        this.jobConfig = jobConfig;
        this._init();
    }

    async start() { // eslint-disable-line class-methods-use-this

    }

    _init() {
        const { exId, jobId } = this.jobConfig;
        const { hostname } = this.context.sysconfig.teraslice;
        const { makeLogger } = this.context.apis.foundation;
        this.workerId = `${hostname}.${shortid.generate()}`;
        this.logger = makeLogger({
            ex_id: exId,
            job_id: jobId,
            module: 'teraslice-worker',
            worker_id: this.workerId,
        });
    }
}

function validateJobConfig(config) {
    if (!isPlainObject(config) || isEmpty(config)) throw new Error('Worker requires a valid job configuration');
    const {
        assignment,
        job,
        exId,
        jobId,
        slicerPort
    } = config;
    if (!isString(assignment)) throw new Error('Job configuration requires a valid assignment');
    if (!isPlainObject(job) || isEmpty(job)) throw new Error('Job configuration requires a valid job');
    if (!isString(exId)) throw new Error('Job configuration requires a valid exId');
    if (!isString(jobId)) throw new Error('Job configuration requires a valid jobId');
    if (!isNumber(slicerPort)) throw new Error('Job configuration requires a valid slicerPort');
}

function generateContext(sysconfig) {
    if (!isPlainObject(sysconfig) || isEmpty(sysconfig)) throw new Error('Worker requires a valid terafoundation configuration');
    const config = getTerasliceConfig({ name: 'teraslice-worker' });
    return makeContext(config, sysconfig);
}

module.exports = Worker;
module.exports.validateJobConfig = validateJobConfig;
module.exports.generateContext = generateContext;
