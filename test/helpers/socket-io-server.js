'use strict';

const Promise = require('bluebird');
const forOwn = require('lodash/forOwn');
const Server = require('socket.io');
const porty = require('porty');

class SocketIOServer {
    constructor() {
        this.events = {
            connection: jest.fn(),
        };
    }

    async start() {
        const port = await porty.find();

        this.server = new Server();

        this.host = `http://localhost:${port}`;
        forOwn(this.events, (mock, event) => {
            this.server.on(event, mock);
        });

        this.server.listen(port);
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = SocketIOServer;
