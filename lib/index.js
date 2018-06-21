'use strict';

const get = require('lodash/get');
const { validateJobConfig, generateContext } = require('./utils');

class TerasliceWorker {
    constructor(config, jobConfig) {
        validateJobConfig(jobConfig);
        this.context = generateContext(config);
        this.jobConfig = jobConfig;
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


module.exports = TerasliceWorker;
