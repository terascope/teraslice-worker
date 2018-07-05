'use strict';

const reader = jest.fn(() => Promise.resolve(Array(100).fill('default-reader-data')));
const newReader = jest.fn(() => Promise.resolve(reader));
const slicer = jest.fn(() => Promise.resolve(Array(100).fill('default-slicer-data')));
const newSlicer = jest.fn(() => Promise.resolve(slicer));

function schema() {
    return {
        exampleProp: {
            doc: 'Specify some example configuration',
            default: 0,
            format(val) {
                if (isNaN(val)) {
                    throw new Error('example-reader exampleProp must be a number.');
                } else if (val < 0) {
                    throw new Error('example-reader exampleProp must be a number greater than or equal to 0.');
                }
            }
        }
    };
}

module.exports = {
    reader,
    newReader,
    schema,
    slicer,
    newSlicer,
};
