'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const Server = require('socket.io');
const { EventEmitter } = require('events');
const porty = require('porty');

class MessengerServer extends EventEmitter {
    constructor(port) {
        super();
        if (!isNumber(port)) {
            throw new Error('MessengerServer requires a valid port');
        }

        this.port = port;
        this.server = new Server();
    }

    async start() {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is in-use`);
        }

        this.server.on('connection', (socket) => {
            socket.emit('handshake');
            this.emit('connection', socket);
        });

        this.server.listen(this.port);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = MessengerServer;
