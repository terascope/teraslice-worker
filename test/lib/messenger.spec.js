'use strict';

const Messenger = require('../../lib/messenger');

describe('Messenger', () => {
    describe('when connected without a host', () => {
        it('should throw an error', () => {
            expect(() => {
                new Messenger(); // eslint-disable-line
            }).toThrowError('Messenger requires a valid host');
        });
    });
});
