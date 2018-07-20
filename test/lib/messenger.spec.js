'use strict';

/* eslint-disable no-console, no-new */

const { EventEmitter } = require('events');
const { formatURL, newId } = require('../../lib/utils');
const WorkerMessenger = require('../../lib/messenger/worker');
const MessengerServer = require('../../lib/messenger/messenger-server');
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
            }).toThrowError('MessengerServer requires a valid port');
        });
    });

    describe('when ExecutionControllerMessenger started twice', () => {
        it('should throw an error', async () => {
            const port = await findPort();
            const exMessenger = new ExecutionControllerMessenger({ port });
            await exMessenger.start();
            await expect(exMessenger.start()).rejects.toThrowError(`Port ${port} is already in-use`);
            await exMessenger.shutdown();
        });
    });

    describe('when constructed with an invalid executionControllerUrl', () => {
        let worker;
        beforeEach(() => {
            worker = new WorkerMessenger({
                executionControllerUrl: 'http://idk.example.com',
                workerId: 'hello',
                events: new EventEmitter(),
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
                actionTimeout: 1000,
                events: new EventEmitter(),
            });

            await exMessenger.start();

            workerId = newId('worker-id');
            worker = new WorkerMessenger({
                workerId,
                events: new EventEmitter(),
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
            await exMessenger.shutdown();
            await worker.shutdown();
        });

        describe('when calling start on the worker again', () => {
            it('should not throw an error', () => expect(worker.start()).resolves.toBeNil());
        });

        it('should have no available workers', () => {
            expect(exMessenger.availableWorkers()).toEqual(0);
        });

        describe('when the worker is ready', () => {
            let enqueuedMsg;

            beforeEach(async () => {
                worker.ready();
                enqueuedMsg = await exMessenger.onceWithTimeout(`worker:enqueue:${workerId}`);
            });

            it('should call worker ready on the exMessenger', () => {
                expect(enqueuedMsg).toEqual({ worker_id: workerId });
            });

            it('should have one client connected', () => {
                expect(exMessenger.availableWorkers()).toEqual(1);
                expect(exMessenger.connectedWorkers()).toEqual(1);
            });

            describe('when sending worker:slice:complete', () => {
                it('should emit worker:slice:complete on the exMessenger', async () => {
                    const msg = await worker.sliceComplete({
                        slice: {
                            slice_id: 'worker-slice-complete'
                        },
                        analytics: 'hello',
                        error: 'hello'
                    });

                    expect(msg).toEqual({
                        slice_id: 'worker-slice-complete',
                        recorded: true,
                    });
                    expect(exMessenger.queue.exists('worker_id', workerId)).toBeTrue();
                });
            });

            describe('when receiving cluster:error:terminal', () => {
                let msg;
                beforeEach((done) => {
                    exMessenger.broadcast('cluster:error:terminal', {
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });

                    const timeout = setTimeout(() => {
                        worker.events.removeAllListeners('worker:shutdown');
                        done();
                    }, 1000);

                    worker.events.once('worker:shutdown', (_msg) => {
                        clearTimeout(timeout);
                        msg = _msg;
                        done();
                    });
                });

                it('should receive the message on the worker', () => {
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });
            });

            describe('when receiving execution:error:terminal', () => {
                let msg;
                beforeEach((done) => {
                    exMessenger.executionTerminal('some-ex-id');

                    const timeout = setTimeout(() => {
                        worker.events.removeAllListeners('worker:shutdown');
                        done();
                    }, 1000);

                    worker.events.once('worker:shutdown', (_msg) => {
                        clearTimeout(timeout);
                        msg = _msg;
                        done();
                    });
                });

                it('should receive the message on the worker', () => {
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                    });
                });
            });

            describe('when receiving finished', () => {
                let msg;
                beforeEach((done) => {
                    exMessenger.executionFinished('some-ex-id');

                    const timeout = setTimeout(() => {
                        worker.events.removeAllListeners('worker:shutdown');
                        done();
                    }, 1000);

                    worker.events.once('worker:shutdown', (_msg) => {
                        clearTimeout(timeout);
                        msg = _msg;
                        done();
                    });
                });

                it('should receive the message on the worker', () => {
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                    });
                });
            });

            describe('when receiving slicer:slice:new', () => {
                describe('when the worker is set as available', () => {
                    beforeEach(() => {
                        worker.available = true;
                    });

                    it('should resolve with correct messages', async () => {
                        const response = exMessenger.sendNewSlice(workerId, {
                            example: 'slice-new-message'
                        });

                        const slice = worker.onceWithTimeout('slicer:slice:new');

                        await expect(response).resolves.toEqual({ willProcess: true });
                        await expect(slice).resolves.toEqual({ example: 'slice-new-message' });
                    });
                });

                describe('when the worker is set as unavailable', () => {
                    beforeEach(() => {
                        worker.available = false;
                    });

                    it('should reject with the correct error messages', async () => {
                        const response = exMessenger.sendNewSlice(workerId, {
                            example: 'slice-new-message'
                        });

                        const slice = worker.onceWithTimeout('slicer:slice:new');

                        await expect(response).rejects.toThrowError(`Worker ${workerId} will not process new slice`);
                        await expect(slice).rejects.toThrowError('Timed out after 1000ms, waiting for event "slicer:slice:new"');
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
    });

    describe('when testing server close', () => {
        describe('when close errors', () => {
            it('should reject with the error', () => {
                const messenger = new MessengerServer({ port: 123 });
                messenger.server = {
                    close: jest.fn(done => done(new Error('oh no')))
                };
                return expect(messenger.close()).rejects.toThrowError('oh no');
            });
        });

        describe('when close errors with Not running', () => {
            it('should resolve', () => {
                const messenger = new MessengerServer({ port: 123 });
                messenger.server = {
                    close: jest.fn(done => done(new Error('Not running')))
                };
                return expect(messenger.close()).resolves.toBeNil();
            });
        });

        describe('when close succeeds', () => {
            it('should resolve', () => {
                const messenger = new MessengerServer({ port: 123 });
                messenger.server = {
                    close: jest.fn(done => done())
                };
                return expect(messenger.close()).resolves.toBeNil();
            });
        });
    });
});
