'use strict';

const Promise = require('bluebird');

function emitMessage(socket, eventName, message) {
    return new Promise((resolve) => {
        socket.emit(eventName, message, (result) => {
            resolve(result);
        });
    });
}

function clientConnect(socket) {
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

module.exports = {
    emitMessage,
    clientConnect
};
