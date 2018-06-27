'use strict';

const forOwn = require('lodash/forOwn');
const Messenger = require('../../lib/messenger');
const SocketIOServer = require('../helpers/socket-io-server');

describe('Messenger', () => {
    describe('when constructed without a host', () => {
        it('should throw an error', () => {
            expect(() => {
                new Messenger(); // eslint-disable-line
            }).toThrowError('Messenger requires a valid host');
        });
    });

    describe('when constructed with an invalid host', () => {
        let messenger;
        beforeEach(() => {
            messenger = new Messenger('http://idk.example.com', {
                timeout: 1000,
                reconnection: false,
            });
        });

        it('connect should throw an error', () => {
            const errMsg = /^Unable to connect to host/;
            return expect(messenger.connect()).rejects.toThrowError(errMsg);
        });
    });

    describe('when constructed with an valid host', () => {
        let messenger;
        let server;
        let events;

        beforeEach(async () => {
            events = {
                error: jest.fn(),
                disconnect: jest.fn(),
                'worker:ready': jest.fn()
            };

            server = new SocketIOServer();
            server.events.connection.mockImplementation((socket) => {
                socket.emit('handshake');
                forOwn(events, (mock, eventName) => {
                    socket.on(eventName, mock);
                });
            });

            await server.start();

            messenger = new Messenger(server.host, {
                timeout: 1000,
                reconnection: false,
            });

            await messenger.connect();
        });

        afterEach(async () => {
            await server.close();
            messenger.close();
        });

        it('should call connect on the server', () => {
            expect(server.events.connection).toHaveBeenCalled();
        });

        describe('when sending worker:ready', () => {
            beforeEach(async () => {
                events['worker:ready'].mockImplementation((msg, cb) => {
                    cb();
                });
                await messenger.send('worker:ready', { worker_id: 'some-random-worker-id' });
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
