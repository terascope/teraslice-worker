'use strict';

/* eslint-disable no-console, no-new */

const porty = require('porty');
const shortid = require('shortid');
const { formatURL } = require('../../lib/utils');
const WorkerMessenger = require('../../lib/messenger/worker');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const { ClusterMasterMessenger } = require('../helpers');

describe('Messenger', () => {
    describe('when worker is constructed without a executionControllerUrl', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger(); // eslint-disable-line
            }).toThrowError('WorkerMessenger requires a valid executionControllerUrl');
        });
    });

    describe('when worker is constructed without a clusterMasterUrl', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger({
                    executionControllerUrl: 'example.com'
                });
            }).toThrowError('WorkerMessenger requires a valid clusterMasterUrl');
        });
    });

    describe('when worker is constructed without a workerId', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger({
                    executionControllerUrl: 'example.com',
                    clusterMasterUrl: 'example.com'
                });
            }).toThrowError('WorkerMessenger requires a valid workerId');
        });
    });

    describe('when executionController is constructed without a port', () => {
        it('should throw an error', () => {
            expect(() => {
                new ExecutionControllerMessenger(); // eslint-disable-line
            }).toThrowError('ExecutionControllerMessenger requires a valid port');
        });
    });

    describe('when executionController started twice', () => {
        it('should throw an error', async () => {
            const port = await porty.find();
            const executionController = new ExecutionControllerMessenger({ port });
            await executionController.start();
            await expect(executionController.start()).rejects.toThrowError(`Port ${port} is already in-use`);
            await executionController.close();
        });
    });

    describe('when constructed with an invalid executionControllerUrl', () => {
        let worker;
        beforeEach(() => {
            worker = new WorkerMessenger({
                executionControllerUrl: 'http://idk.example.com',
                clusterMasterUrl: 'http:idk.example.com',
                workerId: 'hello',
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                }
            });
        });

        it('start should throw an error', () => {
            const errMsg = /^Unable to connect to slicer/;
            return expect(worker.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an invalid clusterMasterUrl', () => {
        let worker;
        let executionController;
        beforeEach(async () => {
            const port = await porty.find();
            executionController = new ExecutionControllerMessenger({ port });

            await executionController.start();

            worker = new WorkerMessenger({
                executionControllerUrl: formatURL('localhost', port),
                clusterMasterUrl: 'http://idk.example.com',
                workerId: 'hello',
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                }
            });
        });

        afterEach(async () => {
            await executionController.close();
            await worker.close();
        });

        it('start should throw an error', async () => {
            const errMsg = /^Unable to connect to cluster master/;
            return expect(worker.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let worker;
        let executionController;
        let workerId;
        let clusterMaster;

        beforeEach(async () => {
            const slicerPort = await porty.find();
            const executionControllerUrl = formatURL('localhost', slicerPort);
            executionController = new ExecutionControllerMessenger({
                port: slicerPort,
                timeoutMs: 1000
            });

            await executionController.start();

            const clusterMasterPort = await porty.find();
            const clusterMasterUrl = formatURL('localhost', clusterMasterPort);
            clusterMaster = new ClusterMasterMessenger({
                port: clusterMasterPort,
                timeoutMs: 1000
            });

            await clusterMaster.start();

            workerId = shortid.generate();
            worker = new WorkerMessenger({
                workerId,
                executionControllerUrl,
                clusterMasterUrl,
                timeoutMs: 1000,
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                },
            });

            await worker.start();
        });

        afterEach(async () => {
            await executionController.close();
            await clusterMaster.close();
            await worker.close();
        });

        describe('when calling start on the worker again', () => {
            it('should not throw an error', () => expect(worker.start()).resolves.toBeNil());
        });

        describe('when the worker is ready', () => {
            let slicerReadyMsg;
            let clusterMasterReadyMsg;

            beforeEach(async () => {
                const workerReady = worker.ready();
                const slicerReady = executionController.onWorkerReady(workerId);
                const clusterMasterReady = clusterMaster.onWorkerReady(workerId);
                await workerReady;
                slicerReadyMsg = await slicerReady;
                clusterMasterReadyMsg = await clusterMasterReady;
            });

            it('should be marked as available', () => {
                expect(worker.available).toBeTrue();
            });

            it('should call worker ready on the executionController', () => {
                expect(slicerReadyMsg).toEqual({ worker_id: workerId });
                expect(clusterMasterReadyMsg).toEqual({ worker_id: workerId });
                expect(executionController.workers).toHaveProperty(workerId);
            });

            it('should return immediately when calling onWorkerReady', () => {
                const msg = { worker_id: workerId };
                return expect(executionController.onWorkerReady(workerId)).resolves.toEqual(msg);
            });

            describe('when sending worker:slice:complete', () => {
                beforeEach(() => {
                    worker.sendToExecutionController('worker:slice:complete', { example: 'worker-slice-complete' });
                });

                it('should emit worker:slice:complete on the executionController', async () => {
                    const msg = await executionController.onMessage(`worker:slice:complete:${workerId}`);
                    expect(msg).toEqual({ example: 'worker-slice-complete' });
                });
            });

            describe('when receiving cluster:error:terminal', () => {
                beforeEach(() => {
                    clusterMaster.sendToWorker(workerId, 'cluster:error:terminal', {
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });

                it('should receive the message on the worker', async () => {
                    const msg = await worker.onMessage('cluster:error:terminal');
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });
            });

            describe('when sending execution:error:terminal', () => {
                beforeEach(() => {
                    worker.sendToClusterMaster('execution:error:terminal', {
                        error: 'execution-error-terminal'
                    });
                });

                it('should receive the message on the cluster master', async () => {
                    const msg = await clusterMaster.onMessage(`execution:error:terminal:${workerId}`);
                    expect(msg).toEqual({
                        error: 'execution-error-terminal'
                    });
                });
            });

            describe('when receiving slicer:slice:recorded', () => {
                beforeEach(() => {
                    executionController.sendToWorker(workerId, 'slicer:slice:recorded', {
                        example: 'slice-recorded-message'
                    });
                });

                it('should emit slicer:slice:recorded on the worker', async () => {
                    const msg = await worker.onMessage('slicer:slice:recorded');
                    expect(msg).toEqual({
                        example: 'slice-recorded-message'
                    });
                });
            });

            describe('when receiving slicer:slice:new', () => {
                describe('when the worker is set as available', () => {
                    let responseMsg;
                    let sliceMsg;

                    beforeEach(async () => {
                        const response = executionController.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                        const slice = worker.onMessage('slicer:slice:new');
                        responseMsg = await response;
                        sliceMsg = await slice;
                    });

                    it('should set available to false', () => {
                        expect(worker.available).toBeFalse();
                    });

                    it('should emit slicer:slice:new on the worker', () => {
                        expect(sliceMsg).toEqual({ example: 'slice-new-message' });
                    });

                    it('executionController should get a will process message back', () => {
                        expect(responseMsg).toEqual({ willProcess: true });
                    });
                });

                describe('when the worker is set as unavailable', () => {
                    let responseMsg;
                    let slice;

                    beforeEach(async () => {
                        const response = executionController.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                        worker.available = false;
                        slice = worker.onMessage('slicer:slice:new');
                        responseMsg = await response;
                    });

                    it('should the response correctly', () => {
                        expect(responseMsg).toEqual({ willProcess: false });
                        const errMsg = 'Timeout waiting for event "slicer:slice:new"';
                        return expect(slice).rejects.toThrowError(errMsg);
                    });
                });
            });

            describe('when waiting for message that will never come', () => {
                it('should throw a timeout error', async () => {
                    expect.hasAssertions();
                    try {
                        await worker.onMessage('mystery:message');
                    } catch (err) {
                        expect(err).not.toBeNil();
                        expect(err.message).toEqual('Timeout waiting for event "mystery:message"');
                        expect(err.code).toEqual(408);
                    }
                });
            });
        });

        describe('when the worker is not ready', () => {
            it('should throw an error', () => {
                expect(() => {
                    executionController.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                }).toThrowError(`Cannot send message to worker ${workerId}`);
            });
        });
    });
});
