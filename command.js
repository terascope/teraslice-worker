#!/usr/bin/env node

'use strict';

const path = require('path');
const yargs = require('yargs');
const Worker = require('./lib/worker');
const { readSysConfig } = require('./lib/terafoundation');
const { generateContext } = require('./lib/utils');

class Command {
    constructor() {
        const {
            configfile,
            assignment,
            job,
        } = this._parseArgs();

        const sysconfig = readSysConfig({ configfile });
        const jobConfig = {
            assignment,
            job,
            ex_id: job.ex_id,
            job_id: job.job_id,
            slicer_port: job.slicer_port
        };

        const context = generateContext(sysconfig);

        if (assignment === 'worker') {
            this.worker = new Worker(context, jobConfig);
        }
    }

    async run() {
        try {
            await this.worker.start();
        } catch (err) {
            console.error(err); // eslint-disable-line no-console
            process.exit(1);
        }
        process.exit(0);
    }

    _parseArgs() { // eslint-disable-line class-methods-use-this
        const { argv } = yargs.usage('Usage: $0 [options]')
            .scriptName('teraslice-worker')
            .help('h')
            .alias('h', 'help')
            .option('j', {
                alias: 'job',
                coerce: JSON.parse,
                default: process.env.EX,
                demandOption: true,
                describe: 'Job configuration in JSON stringified form, defaults to env EX.',
            })
            .option('a', {
                alias: 'assignment',
                choices: ['worker', 'execution_controller'],
                describe: 'Worker type assignment, defaults to env NODE_TYPE.',
                default: process.env.NODE_TYPE || 'worker',
                demandOption: true,
            })
            .option('c', {
                alias: 'configfile',
                describe: 'Terafoundation configuration file to load, defaults to env TERAFOUNDATION_CONFIG.',
                coerce: (arg) => {
                    if (!arg) return '';
                    return path.resolve(arg);
                },
            });

        return argv;
    }
}

new Command().run();
