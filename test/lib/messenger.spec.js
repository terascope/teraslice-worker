'use strict';

/* eslint-disable no-console, no-new */

const porty = require('porty');
const shortid = require('shortid');
const WorkerMessenger = require('../../lib/messenger/worker');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');
const ClusterMaster = require('../helpers/cluster-master');

describe('Messenger', () => {
    describe('when worker is constructed without a slicerUrl', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger(); // eslint-disable-line
            }).toThrowError('WorkerMessenger requires a valid slicerUrl');
        });
    });

    describe('when worker is constructed without a clusterMasterUrl', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger({
                    slicerUrl: 'example.com'
                });
            }).toThrowError('WorkerMessenger requires a valid clusterMasterUrl');
        });
    });

    describe('when worker is constructed without a workerId', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger({
                    slicerUrl: 'example.com',
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

    describe('when constructed with an invalid slicerUrl', () => {
        let client;
        beforeEach(() => {
            client = new WorkerMessenger({
                slicerUrl: 'http://idk.example.com',
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
                slicerUrl: `http://localhost:${port}`,
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
            const slicerUrl = `http://localhost:${slicerPort}`;
            server = new ExecutionControllerMessenger({
                port: slicerPort,
                timeoutMs: 1000
            });

            await server.start();

            const clusterMasterPort = await porty.find();
            const clusterMasterUrl = `http://localhost:${clusterMasterPort}`;
            clusterMaster = new ClusterMaster({
                port: clusterMasterPort,
                timeoutMs: 1000
            });

            await clusterMaster.start();

            workerId = shortid.generate();
            client = new WorkerMessenger({
                workerId,
                slicerUrl,
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
                client.ready();
                slicerReadyMsg = await server.onWorkerReady(workerId);
                clusterMasterReadyMsg = await clusterMaster.onWorkerReady(workerId);
            });

            it('should call worker ready on the server', () => {
                expect(slicerReadyMsg).toEqual({ worker_id: workerId });
                expect(clusterMasterReadyMsg).toEqual({ worker_id: workerId });
                expect(server.workers).toHaveProperty(workerId);
            });

            describe('when sending worker:slice:complete', () => {
                beforeEach(() => {
                    client.sendToSlicer('worker:slice:complete', { example: 'worker-slice-complete' });
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

                it('should recieve the message on the client', async () => {
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
                        error: 'exectution-error-terminal'
                    });
                });

                it('should recieve the message on the cluster master', async () => {
                    const msg = await clusterMaster.onMessage(`execution:error:terminal:${workerId}`);
                    expect(msg).toEqual({
                        error: 'exectution-error-terminal'
                    });
                });
            });

            describe('when receiving slicer:slice:new', () => {
                beforeEach(() => {
                    server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                });

                it('should emit slicer:slice:new on the client', async () => {
                    const msg = await client.onMessage('slicer:slice:new');
                    expect(msg).toEqual({ example: 'slice-new-message' });
                });
            });

            describe('when waiting for message that will never come', () => {
                it('should throw a timeout error', () => {
                    const errMsg = 'Timeout waiting for event "mystery:message"';
                    return expect(client.onMessage('mystery:message')).rejects.toThrowError(errMsg);
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
