'use strict';

const forOwn = require('lodash/forOwn');
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
        let events;
        let onConnectionFn;

        beforeEach(async () => {
            onConnectionFn = jest.fn((socket) => {
                forOwn(events, (mock, eventName) => {
                    socket.on(eventName, mock);
                });
            });

            events = {
                error: jest.fn(),
                disconnect: jest.fn(),
                'worker:ready': jest.fn()
            };
            const port = await porty.find();

            server = new MessengerServer(port);
            server.on('connection', onConnectionFn);

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

        it('should call connect on the server', () => {
            expect(onConnectionFn).toHaveBeenCalled();
        });

        describe('when sending worker:ready', () => {
            beforeEach(async () => {
                events['worker:ready'].mockImplementation((msg, cb) => {
                    cb();
                });
                await client.send('worker:ready', { worker_id: 'some-random-worker-id' });
            });

            it('should emit worker:ready on the server', () => {
                expect(events['worker:ready']).toHaveBeenCalled();
            });

            it('should not emit error or disconnect on the sever', () => {
                expect(events.disconnect).not.toHaveBeenCalled();
                expect(events.error).not.toHaveBeenCalled();
            });
        });
    });
});
