'use strict';

const Promise = require('bluebird');
const isString = require('lodash/isString');
const { Manager } = require('socket.io-client');
const { WrapError } = require('./utils');

class Messenger {
    constructor(host, options) {
        if (!isString(host)) {
            throw new Error('Messenger requires a valid host');
        }
        this.client = new Manager(host, Object.assign({
            autoConnect: false,
        }, options));
    }

    connect() {
        return new Promise((resolve, reject) => {
            this.client.connect((err) => {
                if (err) {
                    reject(new WrapError('Unable to connect to host', err));
                    return;
                }
                resolve();
            });
        });
    }
}

module.exports = Messenger;
