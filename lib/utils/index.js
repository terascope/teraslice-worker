'use strict';

const Promise = require('bluebird');
const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const isArray = require('lodash/isArray');
const isNumber = require('lodash/isNumber');
const isFunction = require('lodash/isFunction');
const get = require('lodash/get');
const size = require('lodash/size');
const shortid = require('shortid');
const { makeContext } = require('../terafoundation');
const { getTerasliceConfig } = require('../teraslice');

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

module.exports = {
    generateContext,
    validateJobConfig,
    analyzeOp,
    getMemoryUsage
};
