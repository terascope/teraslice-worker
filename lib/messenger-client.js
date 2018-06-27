'use strict';

const Promise = require('bluebird');
const isString = require('lodash/isString');
const io = require('socket.io-client');
const { WrapError } = require('./utils');
const Messenger = require('./messenger');

class MessengerClient {
    constructor(host, options) {
        if (!isString(host)) {
            throw new Error('MessengerClient requires a valid host');
        }

        this.socket = io(host, Object.assign({
            forceNew: true,
        }, options));
    }

    async start(handlers) {
        try {
            await this._socketConnect();
        } catch (err) {
            throw new WrapError('Unable to connect to host', err);
        }
        this.messenger = new Messenger(this.socket, handlers);
    }

    async send(eventName, message) {
        await this.messenger.send(eventName, message);
    }

    async close() {
        this.socket.close();
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
