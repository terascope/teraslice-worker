'use strict';

const porty = require('porty');
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

        it('connect should throw an error', () => {
            const errMsg = /^Unable to connect to host/;
            return expect(client.connect()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let client;
        let server;
        let handlers;

        beforeEach(async () => {
            handlers = {
                error: jest.fn(),
                disconnect: jest.fn(),
                'worker:ready': jest.fn(),
                'worker:slice:complete': jest.fn()
            };

            const port = await porty.find();

            server = new MessengerServer(port, handlers);

            await server.start();

            const host = `http://localhost:${server.port}`;

            client = new MessengerClient(host, {
                timeout: 1000,
                reconnection: false,
            });

            await client.connect();
        });

        afterEach(async () => {
            await server.close();
            client.close();
        });

        describe('when sending worker:ready', () => {
            beforeEach(async () => {
                await client.send('worker:ready', { worker_id: 'some-random-worker-id' });
            });

            it('should emit worker:ready on the server', () => {
                expect(handlers['worker:ready']).toHaveBeenCalled();
            });

            it('should not emit error or disconnect on the sever', () => {
                expect(handlers.disconnect).not.toHaveBeenCalled();
                expect(handlers.error).not.toHaveBeenCalled();
            });
        });

        describe('when sending worker:slice:complete', () => {
            beforeEach(async () => {
                await client.send('worker:slice:complete', { worker_id: 'some-random-worker-id' });
            });

            it('should emit worker:slice:complete on the server', () => {
                expect(handlers['worker:slice:complete']).toHaveBeenCalled();
            });

            it('should not emit error or disconnect on the sever', () => {
                expect(handlers.disconnect).not.toHaveBeenCalled();
                expect(handlers.error).not.toHaveBeenCalled();
            });
        });
    });
});
