'use strict';

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
    schema,
    reader: () => Promise.resolve(Array(100).fill('default-reader-data')),
    newReader: () => Promise.resolve(module.exports.reader),
    slicer: () => Promise.resolve(Array(100).fill('default-slicer-data')),
    newSlicer: () => Promise.resolve(module.exports.slicer)
};
