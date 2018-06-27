'use strict';

const porty = require('porty');
const shortid = require('shortid');
const WorkerMessenger = require('../../lib/messenger/worker');
const ExecutionControllerMessenger = require('../../lib/messenger/execution-controller');

describe('WorkerMessenger', () => {
    describe('when constructed without a host', () => {
        it('should throw an error', () => {
            expect(() => {
                new WorkerMessenger(); // eslint-disable-line
            }).toThrowError('WorkerMessenger requires a valid host');
        });
    });

    describe('when constructed with an invalid host', () => {
        let client;
        beforeEach(() => {
            client = new WorkerMessenger({
                host: 'http://idk.example.com',
                options: {
                    timeout: 1000,
                    reconnection: false,
                }
            });
        });

        it('start should throw an error', () => {
            const errMsg = /^Unable to connect to host/;
            return expect(client.start()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let client;
        let server;
        let workerId;

        beforeEach(async () => {
            const port = await porty.find();

            server = new ExecutionControllerMessenger({ port });

            await server.start();

            const host = `http://localhost:${server.port}`;

            workerId = shortid.generate();
            client = new WorkerMessenger({
                workerId,
                host,
                options: {
                    timeout: 1000,
                    reconnection: false,
                },
            });

            await client.start();
        });

        afterEach(async () => {
            await server.close();
            await client.close();
        });

        describe('when sending worker:ready', () => {
            beforeEach(() => {
                client.ready();
            });

            it('should call worker ready on the server', (done) => {
                server.once('worker:ready', (msg) => {
                    expect(msg).toEqual({
                        payload: { worker_id: workerId },
                        worker_id: workerId,
                    });
                    expect(server.workers).toHaveProperty(workerId);
                    done();
                });
            });
        });

        describe('when sending worker:slice:complete', () => {
            beforeEach((done) => {
                client.ready();
                server.once('worker:ready', () => {
                    client.send('worker:slice:complete', { example: 'worker-slice-complete' });
                    done();
                });
            });

            it('should emit worker:slice:complete on the server', (done) => {
                server.once('worker:slice:complete', (msg) => {
                    expect(msg).toEqual({
                        payload: { example: 'worker-slice-complete' },
                        worker_id: workerId,
                    });
                    done();
                });
            });
        });

        describe('when receiving slicer:slice:new', () => {
            beforeEach((done) => {
                client.ready();
                server.once('worker:ready', () => {
                    server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
                    done();
                });
            });

            it('should emit slicer:slice:new on the client', (done) => {
                client.once('slicer:slice:new', (msg) => {
                    expect(msg).toEqual({ example: 'slice-new-message' });
                    done();
                });
            });
        });
    });
});
