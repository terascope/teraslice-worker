'use strict';

const get = require('lodash/get');
const { validateJobConfig, generateContext } = require('./utils');

class TerasliceWorker {
    constructor(config, jobConfig) {
        validateJobConfig(jobConfig);
        this.context = generateContext(config);
        this.jobConfig = jobConfig;
        const { hostname } = this.context.sysconfig.teraslice;
        const { getSystemEvents } = this.context.apis.foundation;
        const clusterId = get(this.context, 'cluster.worker.id');
        this.workerId = `${hostname}__${clusterId}`;
        this.events = getSystemEvents();
    }

    makeLogger(props = {}) {
        const { ex_id: exId, job_id: jobId, type } = this.jobConfig;
        const moduleName = ['teralice-worker', type, ...Object.values(props)].join(':');
        const { makeLogger } = this.context.apis.foundation;
        this.logger = makeLogger({
            ex_id: exId,
            job_id: jobId,
            module: moduleName,
            worker_id: this.workerId,
            ...props
        });
    }
}


module.exports = TerasliceWorker;
