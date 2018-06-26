'use strict';

const Promise = require('bluebird');
const isString = require('lodash/isString');
const SocketIOClient = require('socket.io-client');
const { WrapError } = require('./utils');

class Messenger {
    constructor(host, options) {
        if (!isString(host)) {
            throw new Error('Messenger requires a valid host');
        }
        this.client = new SocketIOClient(host, Object.assign({
            autoConnect: false,
        }, options));
    }

    async connect() {
        await new Promise((resolve, reject) => {
            this.client.connect();
            this.client.once('connect', () => {
                resolve();
            });
            this.client.once('connect_error', (err) => {
                reject(new WrapError('Unable to connect', err));
            });
            this.client.once('connect_timeout', (err) => {
                reject(new WrapError('Connection timeout', err));
            });
        });
    }
}

module.exports = Messenger;
