'use strict';

const Promise = require('bluebird');
const Server = require('socket.io');
const porty = require('porty');

class SocketIOServer {
    async start() {
        const port = await porty.find();

        this.server = new Server(port);

        this.host = `http://localhost:${port}`;
    }

    async close() {
        const close = Promise.promisify(this.server.close, {
            context: this.server
        });
        await close();
    }
}

module.exports = SocketIOServer;
