'use strict';

const util = require('util');
const isError = require('lodash/isError');
const get = require('lodash/get');
const nth = require('lodash/nth');
const NestedError = require('nested-error-stacks');

function TerasliceError(...args) {
    let message = nth(args, 0);
    let error = nth(args, 1);

    if (!error) {
        error = message;
    }

    if (!isError(error)) {
        error = new Error(get(error, 'message', error || 'Unknown Exception'));
        Error.captureStackTrace(error, Error);
    }

    if (!message) {
        message = error.toString();
    } else {
        message = `${message}: ${error.toString()}`;
    }

    NestedError.call(this, message, error);
}


util.inherits(TerasliceError, NestedError);

TerasliceError.prototype.name = 'Error';

module.exports = TerasliceError;
