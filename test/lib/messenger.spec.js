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

    describe('when server is constructed without a port', () => {
        it('should throw an error', () => {
            expect(() => {
                new ExecutionControllerMessenger(); // eslint-disable-line
            }).toThrowError('ExecutionControllerMessenger requires a valid port');
        });
    });

    describe('when server started twice', () => {
        it('should throw an error', async () => {
            const port = await porty.find();
            const server = new ExecutionControllerMessenger({ port });
            await server.start();
            await expect(server.start()).rejects.toThrowError(`Port ${port} is already in-use`);
            await server.close();
        });
    });

    describe('when constructed with an invalid executionControllerUrl', () => {
        let client;
        beforeEach(() => {
            client = new WorkerMessenger({
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
            return expect(client.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an invalid clusterMasterUrl', () => {
        let client;
        let server;
        beforeEach(async () => {
            const port = await porty.find();
            server = new ExecutionControllerMessenger({ port });

            await server.start();

            client = new WorkerMessenger({
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
            await server.close();
            await client.close();
        });

        it('start should throw an error', async () => {
            const errMsg = /^Unable to connect to cluster master/;
            return expect(client.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let client;
        let server;
        let workerId;
        let clusterMaster;

        beforeEach(async () => {
            const slicerPort = await porty.find();
            const executionControllerUrl = formatURL('localhost', slicerPort);
            server = new ExecutionControllerMessenger({
                port: slicerPort,
                timeoutMs: 1000
            });

            await server.start();

            const clusterMasterPort = await porty.find();
            const clusterMasterUrl = formatURL('localhost', clusterMasterPort);
            clusterMaster = new ClusterMasterMessenger({
                port: clusterMasterPort,
                timeoutMs: 1000
            });

            await clusterMaster.start();

            workerId = shortid.generate();
            client = new WorkerMessenger({
                workerId,
                executionControllerUrl,
                clusterMasterUrl,
                timeoutMs: 1000,
                socketOptions: {
                    timeout: 1000,
                    reconnection: false,
                },
            });

            await client.start();
        });

        afterEach(async () => {
            await server.close();
            await clusterMaster.close();
            await client.close();
        });

        describe('when calling start on the worker again', () => {
            it('should not throw an error', () => expect(client.start()).resolves.toBeNil());
        });

        describe('when the worker is ready', () => {
            let slicerReadyMsg;
            let clusterMasterReadyMsg;

            beforeEach(async () => {
                const clientReady = client.ready();
                const slicerReady = server.onWorkerReady(workerId);
                const clusterMasterReady = clusterMaster.onWorkerReady(workerId);
                await clientReady;
                slicerReadyMsg = await slicerReady;
                clusterMasterReadyMsg = await clusterMasterReady;
            });

            it('should be marked as available', () => {
                expect(client.available).toBeTrue();
            });

            it('should call worker ready on the server', () => {
                expect(slicerReadyMsg).toEqual({ worker_id: workerId });
                expect(clusterMasterReadyMsg).toEqual({ worker_id: workerId });
                expect(server.workers).toHaveProperty(workerId);
            });

            it('should return immediately when calling onWorkerReady', () => {
                const msg = { worker_id: workerId };
                return expect(server.onWorkerReady(workerId)).resolves.toEqual(msg);
            });

            describe('when sending worker:slice:complete', () => {
                beforeEach(() => {
                    client.sendToExecutionController('worker:slice:complete', { example: 'worker-slice-complete' });
                });

                it('should emit worker:slice:complete on the server', async () => {
                    const msg = await server.onMessage(`worker:slice:complete:${workerId}`);
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

                it('should receive the message on the client', async () => {
                    const msg = await client.onMessage('cluster:error:terminal');
                    expect(msg).toEqual({
                        ex_id: 'some-ex-id',
                        err: 'cluster-error-terminal'
                    });
                });
            });

            describe('when sending execution:error:terminal', () => {
                beforeEach(() => {
                    client.sendToClusterMaster('execution:error:terminal', {
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

            describe('when receiving slicer:slice:new', () => {
                describe('when the worker is set as available', () => {
                    let responseMsg;
                    let sliceMsg;

                    beforeEach(async () => {
                        const response = server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                        const slice = client.onMessage('slicer:slice:new');
                        responseMsg = await response;
                        sliceMsg = await slice;
                    });

                    it('should emit slicer:slice:new on the client', () => {
                        expect(sliceMsg).toEqual({ example: 'slice-new-message' });
                    });

                    it('server should get a will process message back', () => {
                        expect(responseMsg).toEqual({ willProcess: true });
                    });
                });

                describe('when the worker is set as unavailable', () => {
                    let responseMsg;
                    let sliceMsg;

                    beforeEach(async () => {
                        const response = server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                        client.available = false;
                        const slice = client.onMessage('slicer:slice:new');
                        responseMsg = await response;
                        sliceMsg = await slice;
                    });

                    it('client should emit slicer:slice:new', () => {
                        expect(sliceMsg).toEqual({ example: 'slice-new-message' });
                    });

                    it('server should get a will not process message back', () => {
                        expect(responseMsg).toEqual({ willProcess: false });
                    });
                });
            });

            describe('when waiting for message that will never come', () => {
                it('should throw a timeout error', async () => {
                    expect.hasAssertions();
                    try {
                        await client.onMessage('mystery:message');
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
                    server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                }).toThrowError(`Cannot send message to worker ${workerId}`);
            });
        });
    });
});
