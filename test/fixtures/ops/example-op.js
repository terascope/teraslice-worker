'use strict';

function schema() {
    return {
        exampleProp: {
            doc: 'Specify some example configuration',
            default: 0,
            format(val) {
                if (isNaN(val)) {
                    throw new Error('example-op exampleProp must be a number.');
                } else if (val < 0) {
                    throw new Error('example-op exampleProp must be a number greater than or equal to 0.');
                }
            }
        }
    };
}

module.exports = {
    schema,
    op: () => Promise.resolve(Array(100).fill('default-op-data')),
    newProcessor: () => Promise.resolve(module.exports.op),
};
