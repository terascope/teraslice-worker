#!/usr/bin/env node

'use strict';

/* eslint-disable class-methods-use-this, no-console */

const path = require('path');
const yargs = require('yargs');
const get = require('lodash/get');

const { Worker, ExecutionController } = require('.');
const makeExecutionContext = require('./lib/execution-context');
const { readSysConfig } = require('./lib/terafoundation');
const { generateContext } = require('./lib/utils');
const exitHandler = require('./exit-handler');

class Command {
    constructor() {
        const {
            configfile,
            executionContext,
            nodeType,
            useDebugLogger
        } = this._parseArgs();

        this.executionContext = executionContext;
        this.executionContext.assignment = nodeType;

        const sysconfig = readSysConfig({ configfile });

        this.context = generateContext(sysconfig, useDebugLogger);

        this.logger = this.context.logger;
        this.shutdownTimeout = get(this.context, 'sysconfig.teraslice.shutdown_timeout', 60 * 1000);
    }

    async initialize() {
        this.executionContext = await makeExecutionContext(this.context, this.executionContext);

        if (this.executionContext.assignment === 'worker') {
            this.worker = new Worker(this.context, this.executionContext);
        } else if (this.executionContext.assignment === 'execution_controller') {
            this.worker = new ExecutionController(this.context, this.executionContext);
        }

        await this.worker.initialize();
    }

    async run() {
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
            .option('e', {
                alias: 'executionContext',
                coerce: (arg) => {
                    if (!arg) {
                        throw new Error('Execution context must not be not be empty');
                    }
                    try {
                        return JSON.parse(arg);
                    } catch (err) {
                        throw new Error('Execution context be a valid JSON');
                    }
                },
                default: process.env.EX,
                demandOption: true,
                describe: `Execution context in JSON stringified form.
                Defaults to env EX.`,
            })
            .option('n', {
                alias: 'nodeType',
                default: process.env.NODE_TYPE,
                demandOption: true,
                choices: ['execution_controller', 'worker'],
                describe: `Node Type assignment of worker.
                Defaults to env NODE_TYPE`,
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

async function runCommand() {
    const cmd = new Command();
    await cmd.registerExitHandler();
    await cmd.initialize();
    await cmd.run();
}

runCommand();
