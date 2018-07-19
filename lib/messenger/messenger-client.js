'use strict';

const isEmpty = require('lodash/isEmpty');
const isString = require('lodash/isString');
const { URL } = require('url');
const { Manager } = require('socket.io-client');
const Messenger = require('./messenger');

class MessengerClient extends Messenger {
    constructor(opts) {
        super(opts, 'client');
        const {
            hostUrl,
            socketOptions,
        } = opts;

        if (!isString(hostUrl)) {
            throw new Error('MessengerClient requires a valid hostUrl');
        }

        if (isEmpty(socketOptions)) {
            throw new Error('MessengerClient requires a valid namespace');
        }

        const manager = new Manager(hostUrl, socketOptions);

        const { pathname } = new URL(hostUrl);

        this.socket = manager.socket(pathname, socketOptions);
        this.manager = manager;
    }

    async close() {
        if (this.socket.connected) {
            this.socket.close();
        }
        super.close();
    }

    connect() {
        const { socket } = this;
        if (socket.connected) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            let connectErr;
            let connectInterval;

            function _cleanup() {
                clearInterval(connectInterval);
                socket.removeListener('connect', connect);
                socket.removeListener('connect_error', connectError);
                socket.removeListener('connect_timeout', connectError);
            }

            function connect() {
                _cleanup();
                resolve();
            }

            function connectError(err) {
                connectErr = err;
            }

            socket.on('connect', connect);
            socket.on('connect_error', connectError);
            socket.on('connect_timeout', connectError);

            socket.connect();

            connectInterval = setInterval(() => {
                _cleanup();
                reject(connectErr);
            }, this.actionTimeout).unref();
        });
    }
}

module.exports = MessengerClient;
