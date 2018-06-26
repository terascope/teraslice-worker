'use strict';


const { WrapError } = require('../../../lib/utils');

describe('WrapError', () => {
    describe('when constructed just an string', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new WrapError('Hello there');
            }).toThrowError('Hello there');
        });
    });

    describe('when constructed nothing', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new WrapError();
            }).toThrowError('Unknown Exception');
        });
    });

    describe('when constructed with an string and an error', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new WrapError('Wrapped Error', new Error('Bad news bears'));
            }).toThrowError('Wrapped Error: Error: Bad news bears');
        });
    });

    describe('when constructed with just an error', () => {
        it('should throw an Error', () => {
            expect(() => {
                throw new WrapError(new Error('Bad news bears'));
            }).toThrowError('Error: Bad news bears');
        });
    });
});
