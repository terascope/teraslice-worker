'use strict';

const Messenger = require('../../lib/messenger');

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
            const errMsg = 'Unable to connect: Error: xhr poll error';
            return expect(messenger.connect()).rejects.toThrowError(errMsg);
        });
    });
});
