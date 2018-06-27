'use strict';

const Promise = require('bluebird');
const isPlainObject = require('lodash/isPlainObject');
const isEmpty = require('lodash/isEmpty');
const isError = require('lodash/isError');
const last = require('lodash/last');
const isFunction = require('lodash/isFunction');
const forOwn = require('lodash/forOwn');
const remove = require('lodash/remove');

class Messenger {
    constructor(socket, handlers) {
        if (isEmpty(socket)) {
            throw new Error('Messenger requires valid socket handlers');
        }
        if (isEmpty(handlers) || !isPlainObject(handlers)) {
            throw new Error('Messenger requires valid socket handlers');
        }
        forOwn(handlers, (fn, eventName) => {
            socket.on(eventName, async (...args) => {
                const cb = last(args);
                if (!isFunction(cb)) {
                    await fn(...args);
                    return;
                }
                try {
                    const remainingArgs = remove(args, cb);
                    const result = await fn(...remainingArgs);
                    cb(result);
                } catch (err) {
                    cb(err);
                }
            });
        });
        this.socket = socket;
        this.handlers = handlers;
    }

    send(eventName, message) {
        const { socket } = this;
        return new Promise((resolve, reject) => {
            socket.emit(eventName, message, (result) => {
                if (isError(result)) {
                    reject(result);
                    return;
                }
                resolve(result);
            });
        });
    }
}

module.exports = Messenger;
