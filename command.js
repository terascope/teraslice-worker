#!/usr/bin/env node

'use strict';

const Worker = require('./lib/worker');
const { readSysConfig } = require('./lib/terafoundation');

class Command {
    constructor() {
        let job;
        try {
            job = JSON.parse(process.env.JOB_CONFIGURATION);
        } catch (err) {
            throw new Error('Unable to parse process.env.JOB_CONFIGURATION');
        }
        const assignment = process.env.WORKER_ASSIGNMENT || 'worker';
        const sysconfig = readSysConfig({ });
        const jobConfig = {
            assignment,
            job,
            ex_id: job.ex_id,
            job_id: job.job_id,
            slicer_port: job.slicer_port
        };

        if (assignment === 'worker') {
            this.worker = new Worker(jobConfig, sysconfig);
        } else {
            throw new Error('Invalid WORKER_ASSIGNMENT');
        }
    }

    async run() {
        try {
            await this.worker.run();
        } catch (err) {
            console.error(err); // no-console
            process.exit(1);
        }
        process.exit(0);
    }
}

new Command().run();
