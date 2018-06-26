'use strict';

const isString = require('lodash/isString');
const Promise = require('bluebird');
const SocketIOClient = require('socket.io-client');

class Messenger {
    constructor(host) {
        if (!isString(host)) {
            throw new Error('Messenger requires a valid host');
        }
        this.client = new SocketIOClient(host, {
            autoConnect: false,
        });
        this._connect = Promise.promisify(this.client, { context: this.client });
    }

    connect() {
        return this._connect();
    }
}

module.exports = Messenger;
