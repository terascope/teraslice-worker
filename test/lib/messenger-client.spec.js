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
            let workerReadyFn;
            beforeEach(async () => {
                workerReadyFn = jest.fn();
                server.on(`${workerId}:worker:ready`, workerReadyFn);
                await client.ready();
            });

            it('should add the worker the server', () => {
                expect(server.workers).toHaveProperty(workerId);
            });

            it('should call worker ready on the server', () => {
                expect(workerReadyFn).toHaveBeenCalled();
            });
        });

        describe('when sending worker:slice:complete', () => {
            let sliceCompleteFn;
            beforeEach(async () => {
                sliceCompleteFn = jest.fn();
                server.on('worker:slice:complete', sliceCompleteFn);
                await client.send('worker:slice:complete', { example: 'worker-slice-complete' });
            });

            it('should emit worker:slice:complete on the server', () => {
                expect(sliceCompleteFn).toHaveBeenCalled();
            });
        });

        describe('when receiving slicer:slice:new', () => {
            let sliceNewFn;
            beforeEach(async () => {
                sliceNewFn = jest.fn();
                client.on('slicer:slice:new', sliceNewFn);
                await client.ready();
                await server.sendToWorker(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
            });

            it('should emit slicer:slice:new on the client', () => {
                expect(sliceNewFn).toHaveBeenCalled();
            });
        });
    });
});
