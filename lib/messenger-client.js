'use strict';

const Promise = require('bluebird');
const isString = require('lodash/isString');
const io = require('socket.io-client');
const { WrapError } = require('./utils');

class MessengerClient {
    constructor(host, options) {
        if (!isString(host)) {
            throw new Error('MessengerClient requires a valid host');
        }

        this.socket = io(host, Object.assign({
            forceNew: true,
        }, options));
    }

    async connect() {
        try {
            await this._socketConnect();
        } catch (err) {
            throw new WrapError('Unable to connect to host', err);
        }

        await this._socketOnce('handshake');
    }

    async send(eventName, message) {
        await this._socketEmit(eventName, message);
    }

    close() {
        return this.socket.close();
    }

    _socketOnce(eventName, ...args) {
        const { socket } = this;
        return new Promise((resolve) => {
            socket.once(eventName, ...args, (...result) => {
                resolve(...result);
            });
        });
    }

    _socketEmit(eventName, ...args) {
        const { socket } = this;
        return new Promise((resolve) => {
            socket.emit(eventName, ...args, (...result) => {
                resolve(...result);
            });
        });
    }

    _socketConnect() {
        const { socket } = this;
        return new Promise((resolve, reject) => {
            function _cleanup() {
                socket.removeListener('connect', connect);
                socket.removeListener('connect_error', connectError);
            }
            function connect() {
                _cleanup();
                resolve();
            }
            function connectError(err) {
                _cleanup();
                reject(err);
            }
            socket.once('connect', connect);
            socket.once('connect_error', connectError);
        });
    }
}

module.exports = MessengerClient;
