#!/usr/bin/env node

'use strict';

/* eslint-disable class-methods-use-this, no-console */

const path = require('path');
const yargs = require('yargs');
const get = require('lodash/get');

const { Worker, ExecutionController } = require('.');
const { readSysConfig } = require('./lib/terafoundation');
const { generateContext } = require('./lib/utils');
const exitHandler = require('./exit-handler');

class Command {
    constructor() {
        const {
            configfile,
            assignment,
            job,
            useDebugLogger
        } = this._parseArgs();

        const sysconfig = readSysConfig({ configfile });
        const config = {
            assignment,
            job,
            ex_id: job.ex_id,
            job_id: job.job_id,
            slicer_port: job.slicer_port
        };

        const context = generateContext(sysconfig, useDebugLogger);

        if (assignment === 'worker') {
            this.worker = new Worker(context, config);
        } else if (assignment === 'execution_controller') {
            this.worker = new ExecutionController(context, config);
        }

        this.logger = context.logger;
        this.shutdownTimeout = get(context, 'sysconfig.teraslice.shutdown_timeout', 60 * 1000);
    }

    async run() {
        await this.worker.initialize();

        try {
            await this.worker.run();
        } catch (err) {
            await this.shutdown(err);
            process.exit(1);
        }

        await this.shutdown();
        process.exit(0);
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

    registerExitHandler() {
        exitHandler(
            async (signal, err) => {
                if (err) {
                    await this.shutdown(`${signal} was caught, exiting... ${err.stack}`);
                } else {
                    await this.shutdown(`Exit called due to signal ${signal}, shutting down...`);
                }
            },
            this.shutdownTimeout
        );
    }

    _parseArgs() {
        const { argv } = yargs.usage('Usage: $0 [options]')
            .scriptName('teraslice-worker')
            .version()
            .alias('v', 'version')
            .help()
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
                describe: `Job configuration in JSON stringified form.
                Defaults to env EX.`,
            })
            .option('a', {
                alias: 'assignment',
                choices: ['worker', 'execution_controller'],
                describe: `Worker type assignment.
                Defaults to env NODE_TYPE.`,
                default: process.env.NODE_TYPE || 'worker',
                demandOption: true,
            })
            .option('c', {
                alias: 'configfile',
                describe: `Terafoundation configuration file to load.
                Defaults to env TERAFOUNDATION_CONFIG.`,
                coerce: (arg) => {
                    if (!arg) return '';
                    return path.resolve(arg);
                },
            })
            .option('d', {
                alias: 'useDebugLogger',
                describe: `Override logger with debug logger, for development use only.
                Defaults to env USE_DEBUG_LOGGER.`,
                default: process.env.USE_DEBUG_LOGGER === 'true',
                boolean: true
            })
            .wrap(yargs.terminalWidth());

        return argv;
    }
}

const cmd = new Command();
cmd.registerExitHandler();
cmd.run();
