'use strict';

const porty = require('porty');
const shortid = require('shortid');
const MessengerClient = require('../../lib/messenger-client');
const MessengerServer = require('../../lib/messenger-server');

describe('MessengerClient', () => {
    describe('when constructed without a host', () => {
        it('should throw an error', () => {
            expect(() => {
                new MessengerClient(); // eslint-disable-line
            }).toThrowError('MessengerClient requires a valid host');
        });
    });

    describe('when constructed with an invalid host', () => {
        let client;
        beforeEach(() => {
            client = new MessengerClient('http://idk.example.com', {
                timeout: 1000,
                reconnection: false,
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
        let serverHandlers;
        let clientHandlers;
        let workerId;

        beforeEach(async () => {
            serverHandlers = {
                error: jest.fn(),
                disconnect: jest.fn(),
                'worker:ready': jest.fn(),
                'worker:slice:complete': jest.fn()
            };

            clientHandlers = {
                'slicer:slice:new': jest.fn(),
            };

            const port = await porty.find();

            server = new MessengerServer(port);

            await server.start(serverHandlers);

            const host = `http://localhost:${server.port}`;

            workerId = shortid.generate();
            client = new MessengerClient(host, {
                timeout: 1000,
                reconnection: false,
            }, clientHandlers);

            await client.start(workerId, clientHandlers);
        });

        afterEach(async () => {
            await server.close();
            await client.close();
        });

        describe('when sending worker:ready', () => {
            beforeEach(async () => {
                await client.send('worker:ready', { example: 'worker-ready-message' });
            });

            it('should emit worker:ready on the server', () => {
                expect(serverHandlers['worker:ready']).toHaveBeenCalled();
            });

            it('should not emit error or disconnect on the sever', () => {
                expect(serverHandlers.disconnect).not.toHaveBeenCalled();
                expect(serverHandlers.error).not.toHaveBeenCalled();
            });
        });

        describe('when sending worker:slice:complete', () => {
            beforeEach(async () => {
                await client.send('worker:slice:complete', { example: 'worker-slice-complete' });
            });

            it('should emit worker:slice:complete on the server', () => {
                expect(serverHandlers['worker:slice:complete']).toHaveBeenCalled();
            });

            it('should not emit error or disconnect on the sever', () => {
                expect(serverHandlers.disconnect).not.toHaveBeenCalled();
                expect(serverHandlers.error).not.toHaveBeenCalled();
            });
        });

        describe('when receiving slicer:slice:new', () => {
            beforeEach(async () => {
                await server.send(workerId, 'slicer:slice:new', { example: 'slice-new-message' });
            });

            it('should emit slicer:slice:new on the server', () => {
                expect(clientHandlers['slicer:slice:new']).toHaveBeenCalled();
            });
        });
    });
});
