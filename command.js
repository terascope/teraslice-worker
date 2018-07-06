#!/usr/bin/env node

'use strict';

/* eslint-disable class-methods-use-this, no-console */

const path = require('path');
const diehard = require('diehard');
const yargs = require('yargs');
const get = require('lodash/get');

const { Worker, ExecutionController } = require('.');
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
        } else if (assignment === 'execution_controller') {
            this.worker = new ExecutionController(context, jobConfig);
        }

        this.logger = context.logger;
        this.shutdownTimeout = get(context, 'sysconfig.teraslice.shutdown_timeout', 60 * 1000);
    }

    async run() {
        await this.worker.initialize();

        try {
            await this.worker.start();
        } catch (err) {
            this.shutdown(err);
            process.exit(1);
        }
        process.exit(0);
    }

    handleExit() {
        diehard.register(async (signal, done) => {
            this.logger.warn(`Exit called due to signal ${signal}, shutting down...`);
            await this.shutdown();
            done();
        });

        diehard.listen({
            uncaughtException: false,
            timeout: this.shutdownTimeout,
        });
    }


    handleErrors() {
        process.on('uncaughtException', async (err) => {
            await this.shutdown(err);
            process.exit(1);
        });

        process.on('unhandledRejection', async (err) => {
            await this.shutdown(err);
            process.exit(1);
        });
    }

    async shutdown(err) {
        if (err) {
            this.logError(err);
        }

        try {
            await this.worker.shutdown();
        } catch (shutdowErr) {
            this.logError(shutdowErr);
        }

        try {
            await this.logger.flush();
            // hack for logger to flush
            await Promise.delay(600);
        } catch (flushErr) {
            this.logError(flushErr);
        }
    }

    logError(err) {
        const logErr = this.logger ? this.logger.error.bind(this.logger) : console.error;
        if (err.message) {
            logErr(err.message);
        } else {
            logErr(err);
        }

        if (err.stack) {
            logErr(err.stack);
        }
    }

    _parseArgs() {
        const { argv } = yargs.usage('Usage: $0 [options]')
            .scriptName('teraslice-worker')
            .help('h')
            .alias('h', 'help')
            .option('j', {
                alias: 'job',
                coerce: (arg) => {
                    if (!arg) {
                        throw new Error('Job configuration must not be not be empty');
                    }
                    try {
                        return JSON.parse(arg);
                    } catch (err) {
                        throw new Error('Job configuration be a valid JSON');
                    }
                },
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

const cmd = new Command();
cmd.handleExit();
cmd.handleErrors();
cmd.run();
