'use strict';

const defaultReaderSchema = require('../fixtures/ops/example-reader').schema;
const defaultOpSchema = require('../fixtures/ops/example-op').schema;

module.exports = (options = {}) => {
    jest.doMock('../fixtures/ops/example-reader', () => {
        const {
            slicers = [() => null],
            reader = jest.fn(() => Promise.resolve(Array(10).fill('hello'))),
        } = options;

        const {
            newReader = jest.fn(() => Promise.resolve(reader)),
            newSlicer = jest.fn(() => Promise.resolve(slicers)),
        } = options;

        return {
            schema: jest.fn(defaultReaderSchema),
            reader,
            newReader,
            newSlicer,
        };
    });

    jest.doMock('../fixtures/ops/example-op', () => {
        const {
            op = jest.fn(() => Array(10).fill('hi')),
        } = options;
        const {
            newProcessor = jest.fn(() => Promise.resolve(op))
        } = options;
        return {
            schema: jest.fn(defaultOpSchema),
            op,
            newProcessor,
        };
    });

    return {
        exampleReader: require('../fixtures/ops/example-reader'),
        exampleOp: require('../fixtures/ops/example-op'),
    };
};
