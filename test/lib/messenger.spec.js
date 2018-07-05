'use strict';

/* eslint-disable no-console, no-new */

const { formatURL, newId } = require('../../lib/utils');
const WorkerMessenger = require('../../lib/messenger/worker');
const { closeServer } = require('../../lib/messenger/helpers');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const { findPort } = require('../helpers');

describe('Messenger', () => {
    describe('when worker is constructed without a executionControllerUrl', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger(); // eslint-disable-line
            }).toThrowError('WorkerMessenger requires a valid executionControllerUrl');
        });
    });

    describe('when WorkerMessenger is constructed without a workerId', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger({
                    executionControllerUrl: 'example.com'
                });
            }).toThrowError('WorkerMessenger requires a valid workerId');
        });
    });

    describe('when ExecutionControllerMessenger is constructed without a port', () => {
        it('should throw an error', () => {
            expect(() => {
                new ExecutionControllerMessenger(); // eslint-disable-line
            }).toThrowError('ExecutionControllerMessenger requires a valid port');
        });
    });

    describe('when ExecutionControllerMessenger started twice', () => {
        it('should throw an error', async () => {
            const port = await findPort();
            const exMessenger = new ExecutionControllerMessenger({ port });
            await exMessenger.start();
            await expect(exMessenger.start()).rejects.toThrowError(`Port ${port} is already in-use`);
            await exMessenger.close();
        });
    });

    describe('when constructed with an invalid executionControllerUrl', () => {
        let worker;
        beforeEach(() => {
            worker = new WorkerMessenger({
                executionControllerUrl: 'http://idk.example.com',
                workerId: 'hello',
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                }
            });
        });

        it('start should throw an error', () => {
            const errMsg = /^Unable to connect to execution controller/;
            return expect(worker.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let worker;
        let exMessenger;
        let workerId;

        beforeEach(async () => {
            const slicerPort = await findPort();
            const executionControllerUrl = formatURL('localhost', slicerPort);
            exMessenger = new ExecutionControllerMessenger({
                port: slicerPort,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000
            });

            await exMessenger.start();

            workerId = newId('worker-id');
            worker = new WorkerMessenger({
                workerId,
                executionControllerUrl,
                networkerLatencyBuffer: 0,
                actionTimeout: 1000,
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                },
            });

            await worker.start();
        });

        afterEach(async () => {
            await exMessenger.close();
            await worker.close();
        });

        describe('when calling start on the worker again', () => {
            it('should not throw an error', () => expect(worker.start()).resolves.toBeNil());
        });

        describe('when the worker is ready', () => {
            let slicerReadyMsg;

            beforeEach(async () => {
                const workerReady = worker.ready();
                const slicerReady = exMessenger.onWorkerReady(workerId);
                await workerReady;
                slicerReadyMsg = await slicerReady;
            });

            it('should be marked as available', () => {
                expect(worker.available).toBeTrue();
            });

            it('should call worker ready on the exMessenger', () => {
                expect(slicerReadyMsg).toEqual({ worker_id: workerId });
                expect(exMessenger.workers).toHaveProperty(workerId);
            });

            it('should return immediately when calling onWorkerReady', () => {
                const msg = { worker_id: workerId };
                return expect(exMessenger.onWorkerReady(workerId)).resolves.toEqual(msg);
            });

            describe('when sending worker:slice:complete', () => {
                beforeEach(() => {
                    worker.sliceComplete({ slice: 'worker-slice-complete', analyticsData: 'hello', error: 'hello' });
                });

                it('should emit worker:slice:complete on the exMessenger', async () => {
                    const msg = await exMessenger.onSliceComplete(workerId);
                    expect(msg).toEqual({
                        slice: 'worker-slice-complete',
                        analytics: 'hello',
                        error: 'hello',
                        worker_id: workerId,
                    });
                });
            });

            describe('when slice complete should work when using the cache', () => {
                beforeEach(async () => {
                    worker.sliceComplete({ slice: 'hello-1', analyticsData: 'hello', error: 'hello' });
                    worker.sliceComplete({ slice: 'hello-2', analyticsData: 'hello', error: 'hello' });
                    await Promise.delay(500);
                });

                it('should emit worker:slice:complete on the exMessenger', async () => {
                    const msg = await exMessenger.onSliceComplete(workerId);
                    expect(msg).toEqual({
                        slice: 'hello-1',
                        analytics: 'hello',
                        error: 'hello',
                        worker_id: workerId,
                    });
                    const msg2 = await exMessenger.onSliceComplete(workerId);
                    expect(msg2).toEqual({
                        slice: 'hello-2',
                        analytics: 'hello',
                        error: 'hello',
                        worker_id: workerId,
                    });
                });
            });

            describe('when receiving cluster:error:terminal', () => {
                beforeEach(() => {
                    exMessenger.sendToWorker(workerId, 'cluster:error:terminal', {
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });

                it('should receive the message on the worker', async () => {
                    const msg = await worker.onceWithTimeout('cluster:error:terminal');
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });
            });

            xdescribe('when sending execution:error:terminal', () => {
                beforeEach(() => {
                    exMessenger.send('execution:error:terminal', {
                        error: 'execution-error-terminal'
                    });
                });

                it('should receive the message on the cluster master', async () => {
                    // eslint-disable-next-line no-undef
                    const msg = await cmMessenger.onceWithTimeout(`execution:error:terminal:${workerId}`);
                    expect(msg).toEqual({
                        error: 'execution-error-terminal'
                    });
                });
            });

            describe('when receiving slicer:slice:recorded', () => {
                beforeEach(() => {
                    exMessenger.sendToWorker(workerId, 'slicer:slice:recorded', {
                        example: 'slice-recorded-message'
                    });
                });

                it('should emit slicer:slice:recorded on the worker', async () => {
                    const msg = await worker.onceWithTimeout('slicer:slice:recorded');
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
                        const response = exMessenger.sendNewSlice(workerId, {
                            example: 'slice-new-message'
                        });
                        const slice = worker.onceWithTimeout('slicer:slice:new');
                        responseMsg = await response;
                        sliceMsg = await slice;
                    });

                    it('should set available to false', () => {
                        expect(worker.available).toBeFalse();
                    });

                    it('should emit slicer:slice:new on the worker', () => {
                        expect(sliceMsg).toEqual({ example: 'slice-new-message' });
                    });

                    it('exMessenger should get a will process message back', () => {
                        expect(responseMsg).toEqual({ willProcess: true });
                    });
                });


                describe('when the worker is set as unavailable', () => {
                    let responseMsg;
                    let slice;

                    beforeEach(async () => {
                        const response = exMessenger.sendNewSlice(workerId, { example: 'slice-new-message' });
                        worker.available = false;
                        slice = worker.onceWithTimeout('slicer:slice:new');
                        responseMsg = await response;
                    });

                    it('should the response correctly', () => {
                        expect(responseMsg).toEqual({ willProcess: false });
                        const errMsg = 'Timed out after 1000ms, waiting for event "slicer:slice:new"';
                        return expect(slice).rejects.toThrowError(errMsg);
                    });
                });
            });

            describe('when waiting for message that will never come', () => {
                it('should throw a timeout error', async () => {
                    expect.hasAssertions();
                    try {
                        await worker.onceWithTimeout('mystery:message');
                    } catch (err) {
                        expect(err).not.toBeNil();
                        expect(err.message).toEqual('Timed out after 1000ms, waiting for event "mystery:message"');
                        expect(err.code).toEqual(408);
                    }
                });
            });

            describe('when the worker responds with an error', () => {
                let responseMsg;
                let responseErr;

                beforeEach(async () => {
                    worker.socket.on('some:message', (msg) => {
                        worker.socket.emit('messaging:response', {
                            __msgId: msg.__msgId,
                            __source: 'execution_controller',
                            error: 'this should fail'
                        });
                    });
                    try {
                        responseMsg = await exMessenger.sendWithResponse(workerId, 'some:message', { hello: true });
                    } catch (err) {
                        responseErr = err;
                    }
                });

                it('exMessenger should get an error back', () => {
                    expect(responseMsg).toBeNil();
                    expect(responseErr.toString()).toEqual('Error: this should fail');
                });
            });

            describe('when the worker takes too long to respond', () => {
                let responseMsg;
                let responseErr;

                beforeEach(async () => {
                    try {
                        responseMsg = await exMessenger.sendWithResponse(workerId, 'some:message', { hello: true });
                    } catch (err) {
                        responseErr = err;
                    }
                });

                it('exMessenger should get an error back', () => {
                    expect(responseMsg).toBeNil();
                    expect(responseErr.toString()).toStartWith(`Error: Timeout error while communicating with ${workerId}, with message:`);
                });
            });
        });

        describe('when the worker is not ready', () => {
            describe('when sending a slice', () => {
                it('should throw an error', () => {
                    const errMsg = `Cannot send message to worker ${workerId}`;
                    const promise = exMessenger.sendNewSlice(workerId, { example: 'slice-new-message' });
                    return expect(promise).rejects.toThrowError(errMsg);
                });
            });

            describe('when sending a slice', () => {
                it('should throw an error', () => {
                    const errMsg = `Cannot send message to worker ${workerId}`;
                    const promise = exMessenger.sendToWorker(workerId, 'some:event', { example: true });
                    return expect(promise).rejects.toThrowError(errMsg);
                });
            });
        });
    });

    describe('when testing server close', () => {
        describe('when close errors', () => {
            it('should reject with the error', () => {
                const server = {
                    close: jest.fn(done => done(new Error('oh no')))
                };
                return expect(closeServer(server)).rejects.toThrowError('oh no');
            });
        });

        describe('when close errors with Not running', () => {
            it('should resolve', () => {
                const server = {
                    close: jest.fn(done => done(new Error('Not running')))
                };
                return expect(closeServer(server)).resolves.toBeNil();
            });
        });

        describe('when close succeeds', () => {
            it('should resolve', () => {
                const server = {
                    close: jest.fn(done => done()),
                };
                return expect(closeServer(server)).resolves.toBeNil();
            });
        });
    });
});
