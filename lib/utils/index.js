'use strict';

const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const isArray = require('lodash/isArray');
const isNumber = require('lodash/isNumber');
const isFunction = require('lodash/isFunction');
const forOwn = require('lodash/forOwn');
const get = require('lodash/get');
const size = require('lodash/size');
const shortid = require('shortid');
const url = require('url');
const { makeContext } = require('../terafoundation');
const { getTerasliceConfig } = require('../teraslice');
const WrapError = require('./wrap-error');

function validateJobConfig(config) {
    if (!isPlainObject(config) || isEmpty(config)) {
        throw new Error('Worker requires a valid job configuration');
    }

    const {
        assignment,
        job,
        ex_id: exId,
        job_id: jobId,
        slicer_port: slicerPort
    } = config;

    if (!isString(assignment)) {
        throw new Error('Job configuration requires a valid assignment');
    }
    if (!isPlainObject(job) || isEmpty(job)) {
        throw new Error('Job configuration requires a valid job');
    }
    if (!isString(exId)) {
        throw new Error('Job configuration requires a valid ex_id');
    }
    if (!isString(jobId)) {
        throw new Error('Job configuration requires a valid job_id');
    }
    if (!isNumber(slicerPort)) {
        throw new Error('Job configuration requires a valid slicer_port');
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


function getMemoryUsage() {
    return process.memoryUsage().heapUsed;
}

function analyzeOp(fn, index) {
    if (!isFunction(fn)) throw new Error('Operation analytics requires a valid op function');
    if (!isNumber(index)) throw new Error('Operation analytics requires a valid index');
    return (obj, data, logger, msg) => {
        const start = Date.now();
        let end;
        let startingMemory = getMemoryUsage();

        function compareMemoryUsage() {
            const used = getMemoryUsage();
            const diff = used - startingMemory;
            // set the starting point for next op based off of what is used
            startingMemory = used;
            return diff;
        }

        return Promise.resolve(fn(data, logger, msg))
            .then((result) => {
                end = Date.now();
                obj.time[index] = (end - start);
                obj.memory[index] = compareMemoryUsage();
                const results = get(result, 'hits.hits', result);
                if (isArray(results)) {
                    obj.size[index] = size(results);
                } else {
                    obj.size[index] = 0;
                }
                return result;
            });
    };
}

function logOpStats(logger, slice, analyticsData) {
    const str = 'analytics for slice ';
    let dataStr = '';

    if (typeof slice === 'string') {
        dataStr = `${slice}, `;
    } else {
        forOwn(slice, (value, key) => {
            dataStr += `${key} : ${value} `;
        });
    }
    forOwn(analyticsData, (value, key) => {
        dataStr += `${key} : ${value} `;
    });

    logger.info(str + dataStr);
}

function formatURL(hostname, port) {
    let formatOptions;
    try {
        const parsed = new url.URL(hostname);
        formatOptions = Object.assign(parsed, {
            port,
        });
    } catch (err) {
        formatOptions = {
            protocol: 'http:',
            slashes: true,
            hostname,
            port,
        };
    }

    return url.format(formatOptions);
}

function generateWorkerId(context) {
    const { hostname } = context.sysconfig.teraslice;
    const clusterId = get(context, 'cluster.worker.id');
    return `${hostname}__${clusterId}`;
}

function makeLogger(context, jobConfig, extra = {}) {
    const { ex_id: exId, job_id: jobId, type } = jobConfig;
    const moduleName = ['teraslice-worker', type, ...Object.values(extra)].join(':');
    const workerId = generateWorkerId(context);
    return context.apis.foundation.makeLogger({
        ex_id: exId,
        job_id: jobId,
        module: moduleName,
        worker_id: workerId,
        ...extra
    });
}

module.exports = {
    generateWorkerId,
    makeLogger,
    formatURL,
    logOpStats,
    generateContext,
    validateJobConfig,
    analyzeOp,
    getMemoryUsage,
    WrapError
};
