'use strict';

const _ = require('lodash');
const { makeExecutionRecovery } = require('.');

module.exports = function makeEngine(controller) {
    const {
        logger,
        executionContext,
        events,
        context,
        slicerFailure,
    } = controller;

    const {
        ex_id: exId,
        config: executionConfig,
    } = executionContext;

    const { stateStore } = controller.stores;
    let scheduler;

    const recovery = makeExecutionRecovery(context, slicerFailure, stateStore, executionContext);

    async function executionRecovery() {
        const recoverExecution = executionConfig.recovered_execution;
        if (!recoverExecution) return [];

        logger.info(`execution: ${exId} is starting in recovery mode`);
        await recovery.initialize();

        return new Promise((resolve, reject) => {
            events.once('execution:recovery:complete', (executionStartingPoints) => {
                resolve(executionStartingPoints);
            });

            Promise.resolve()
                .then(recovery.newSlicer)
                .then(slicerArray => registerSlicers(slicerArray, 'recovery'))
                .catch(err => reject(err));
        });
    }

    function registerSlicers(slicersArray, isRecovery) {
        if (!Array.isArray(slicersArray)) {
            throw new Error(`newSlicer from module ${executionConfig.operations[0]._op} needs to return an array of slicers`);
        }

        controller.executionAnalytics.set('slicers', slicersArray.length);
        scheduler = _getScheduler(slicersArray);

        // Recovery has it own error listening logic internally
        if (!isRecovery) {
            if (executionConfig.lifecycle === 'once') {
                events.once('slice:failure', controller.setFailingStatus);
            } else {
                // in persistent mode we set watchdogs to monitor
                // when failing can be set back to running
                events.on('slice:failure', _checkAndUpdateExecutionState());
            }
        }
    }

    function _checkAndUpdateExecutionState() {
        let watchDogSet = false;
        let errorCount;
        let processedCount;
        let watcher;

        return async () => {
            if (watchDogSet) return;
            watchDogSet = true;
            const analyticsData = controller.executionAnalytics.getAnalytics();
            // keep track of how many slices have been processed and failed
            errorCount = analyticsData.failed;
            processedCount = analyticsData.processed;
            await controller.setFailingStatus();
            const { exStore } = controller.stores;

            watcher = setInterval(() => {
                const currentAnalyticsData = controller.executionAnalytics.getAnalytics();
                const currentErrorCount = currentAnalyticsData.failed;
                const currentProcessedCount = currentAnalyticsData.processed;
                const errorCountTheSame = currentErrorCount === errorCount;
                const slicesHaveProcessedSinceError = currentProcessedCount > processedCount;

                if (errorCountTheSame && slicesHaveProcessedSinceError) {
                    clearInterval(watcher);
                    logger.info(`No slice errors have occurred within execution: ${exId} will be set back to 'running' state`);
                    exStore.setStatus(exId, 'running');
                    return;
                }
                errorCount = currentErrorCount;
                processedCount = currentProcessedCount;
            }, executionConfig.probation_window);
        };
    }

    function _getScheduler(slicerArray) {
        const { lifecycle } = executionConfig;
        return slicerArray.map((slicerFn, index) => _createSlices(slicerFn, index, lifecycle));
    }

    function _createSlices(slicerFn, slicerId, lifecycle) {
        let hasCompleted = false;
        let isProcessing = false;
        let slicerOrder = 0;

        // checking if lifecycle is 'once' and not in recovery mode
        const isOnce = (lifecycle === 'once') && recovery.recoveryComplete();

        return async function createSliceFn() {
            if (!isProcessing && !hasCompleted) {
                logger.trace(`slicer ${slicerId} is being called`);
                isProcessing = true;
                try {
                    const sliceRequest = await slicerFn();
                    // not null or undefined
                    if (sliceRequest != null) {
                        if (_.isArray(sliceRequest)) {
                            logger.warn(`slicer for execution: ${exId} is subslicing by key`);
                            controller.executionAnalytics.increment('subslice_by_key');
                        }

                        slicerOrder = await controller.allocateSlice(
                            sliceRequest,
                            slicerId,
                            slicerOrder
                        );
                    } else if (isOnce) {
                        // slicer => a single slicer has finished
                        events.emit('slicer:finished');
                        hasCompleted = true;
                        await controller.slicerCompleted();
                    }

                    isProcessing = false;
                } catch (err) {
                    // retries are handled internally by slicer
                    isProcessing = false;
                    controller.slicerFailure(err);
                }
            }
        };
    }

    async function shutdown() {
        scheduler = null;
        if (!recovery) return;
        await recovery.shutdown();
    }

    function getScheduler() {
        return scheduler;
    }

    return {
        registerSlicers,
        executionRecovery,
        getScheduler,
        shutdown,
    };
};
