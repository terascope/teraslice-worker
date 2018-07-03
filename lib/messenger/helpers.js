'use strict';

const Promise = require('bluebird');

function clientConnect(socket) {
    if (socket.connected) {
        return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
        function _cleanup() {
            socket.removeListener('connect', connect);
            socket.removeListener('connect_error', connectError);
            socket.removeListener('connect_timeout', connectError);
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
        socket.once('connect_timeout', connectError);

        socket.connect();
    });
}

function closeServer(server) {
    return new Promise((resolve, reject) => {
        server.close((err) => {
            if (err && err.toString() === 'Error: Not running') {
                resolve();
                return;
            }
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}

module.exports = {
    closeServer,
    clientConnect
};
