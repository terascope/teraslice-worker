'use strict';

const _ = require('lodash');
const nanoid = require('nanoid/generate');
const url = require('url');
const { makeContext } = require('../terafoundation');
const { getTerasliceConfig } = require('../teraslice');
const WrapError = require('./wrap-error');

function validateJobConfig(config) {
    if (!_.isPlainObject(config) || _.isEmpty(config)) {
        throw new Error('Worker requires a valid job configuration');
    }

    const {
        assignment,
        job,
        ex_id: exId,
        job_id: jobId,
        slicer_port: slicerPort
    } = config;

    if (!_.isString(assignment)) {
        throw new Error('Job configuration requires a valid assignment');
    }

    if (!_.includes(['worker', 'execution_controller'], assignment)) {
        throw new Error('Job configuration requires assignment to worker, or execution_controller');
    }

    if (!_.isPlainObject(job) || _.isEmpty(job)) {
        throw new Error('Job configuration requires a valid job');
    }
    if (!_.isString(exId)) {
        throw new Error('Job configuration requires a valid ex_id');
    }
    if (!_.isString(jobId)) {
        throw new Error('Job configuration requires a valid job_id');
    }
    if (!_.isNumber(slicerPort)) {
        throw new Error('Job configuration requires a valid slicer_port');
    }
}

function generateContext(sysconfig) {
    if (!_.isPlainObject(sysconfig) || _.isEmpty(sysconfig)) {
        throw new Error('Worker requires a valid terafoundation configuration');
    }

    const config = getTerasliceConfig({ name: 'teraslice-worker' });
    const cluster = {
        worker: {
            id: newId()
        }
    };
    return makeContext(cluster, config, sysconfig);
}


function getMemoryUsage() {
    return process.memoryUsage().heapUsed;
}

function analyzeOp(fn, index) {
    if (!_.isFunction(fn)) throw new Error('Operation analytics requires a valid op function');
    if (!_.isNumber(index)) throw new Error('Operation analytics requires a valid index');
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
                const results = _.get(result, 'hits.hits', result);
                if (_.isArray(results)) {
                    obj.size[index] = _.size(results);
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
        _.forOwn(slice, (value, key) => {
            dataStr += `${key} : ${value} `;
        });
    }
    _.forOwn(analyticsData, (value, key) => {
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
    const clusterId = _.get(context, 'cluster.worker.id');
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

function newId(prefix, lowerCase = false, length = 15) {
    let characters = '_-0123456789abcdefghijklmnopqrstuvwxyz';
    if (!lowerCase) {
        characters += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    }
    const id = nanoid(characters, length);
    if (prefix) {
        return `${prefix}-${id}`;
    }
    return id;
}

module.exports = {
    newId,
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
