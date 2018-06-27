'use strict';

const Promise = require('bluebird');
const isNumber = require('lodash/isNumber');
const omitBy = require('lodash/omitBy');
const Server = require('socket.io');
const porty = require('porty');
const Messenger = require('./messenger');

class MessengerServer {
    constructor(port) {
        if (!isNumber(port)) {
            throw new Error('MessengerServer requires a valid port');
        }
        this.port = port;
        this.messengers = {};
        this.idMapping = {};
        this.server = new Server();
    }

    async start(handlers) {
        const portAvailable = await porty.test(this.port);
        if (!portAvailable) {
            throw new Error(`Port ${this.port} is in-use`);
        }

        this.server.on('connection', (socket) => {
            const messenger = new Messenger(socket, handlers);
            socket.on('disconnect', () => {
                this.idMapping = omitBy(this.idMapping, id => id === socket.id);
                this.messengers[socket.id] = null;
            });
            socket.on('add:mapping', (id, cb) => {
                this.idMapping[id] = socket.id;
                cb();
            });
            this.idMapping[socket.id] = socket.id;
            this.messengers[socket.id] = messenger;
        });

        this.server.listen(this.port);
    }

    async send(id, eventName, message) {
        const socketId = this.idMapping[id];
        if (!socketId) {
            throw new Error(`No socket found by id ${id}`);
        }
        return this.messengers[socketId].send(eventName, message);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = MessengerServer;
