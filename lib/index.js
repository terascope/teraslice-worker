'use strict';

const {
    isPlainObject,
    isEmpty,
    isString,
    isNumber,
    get,
} = require('lodash');

const shortid = require('shortid');
const { makeContext } = require('./terafoundation');
const { getTerasliceConfig } = require('./teraslice');

class TerasliceWorker {
    constructor(config, jobConfig) {
        validateJobConfig(jobConfig);
        this.context = generateContext(config);
        this.jobConfig = jobConfig;
        this._init();
    }

    _init() {
        const { exId, jobId, type } = this.jobConfig;
        const { hostname } = this.context.sysconfig.teraslice;
        const { makeLogger, getSystemEvents } = this.context.apis.foundation;
        const clusterId = get(this.context, 'cluster.worker.id');
        this.workerId = `${hostname}__${clusterId}`;
        this.logger = makeLogger({
            ex_id: exId,
            job_id: jobId,
            module: `teraslice-worker:${type}`,
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
        type,
        job,
        exId,
        jobId,
        slicerPort
    } = config;

    if (!isString(type)) {
        throw new Error('Job configuration requires a valid type');
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
}

function generateContext(sysconfig) {
    if (!isPlainObject(sysconfig) || isEmpty(sysconfig)) {
        throw new Error('Worker requires a valid terafoundation configuration');
    }

    const config = getTerasliceConfig({ name: 'teraslice-worker' });
    const cluster = {
        worker: {
            id: shortid.generate()
        }
    };
    return makeContext(cluster, config, sysconfig);
}

module.exports = TerasliceWorker;
module.exports.validateJobConfig = validateJobConfig;
module.exports.generateContext = generateContext;
