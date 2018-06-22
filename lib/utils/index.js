'use strict';


const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const isNumber = require('lodash/isNumber');
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

module.exports = { generateContext, validateJobConfig };
