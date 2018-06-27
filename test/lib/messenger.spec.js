'use strict';

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

        beforeEach(async () => {
            server = new SocketIOServer();
            await server.start();
            messenger = new Messenger(server.host, {
                timeout: 1000,
                reconnection: false,
            });
            await messenger.connect();
        });

        afterEach(async () => {
            await server.close();
        });

        it('should return a client', () => {
            expect(messenger).not.toBeNil();
        });
    });
});
