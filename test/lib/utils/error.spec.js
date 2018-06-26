'use strict';


const TerasliceError = require('../../../lib/utils/error');

describe('TerasliceError', () => {
    describe('when constructed just an string', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new TerasliceError('Hello there');
            }).toThrowError('Hello there');
        });
    });

    describe('when constructed nothing', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new TerasliceError();
            }).toThrowError('Unknown Exception');
        });
    });

    describe('when constructed with an string and an error', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new TerasliceError('Wrapped Error', new Error('Bad news bears'));
            }).toThrowError('Wrapped Error: Error: Bad news bears');
        });
    });

    describe('when constructed with just an error', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new TerasliceError(new Error('Bad news bears'));
            }).toThrowError('Error: Bad news bears');
        });
    });
});
