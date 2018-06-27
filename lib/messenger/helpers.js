'use strict';

const Promise = require('bluebird');
const isError = require('lodash/isError');

async function emitMessage(socket, eventName, message) {
    return new Promise((resolve, reject) => {
        socket.emit(eventName, message, (result) => {
            if (isError(result)) {
                reject(result);
                return;
            }
            resolve(result);
        });
    });
}

function broadcastMessage(socket, eventName, message) {
    socket.emit(eventName, message);
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
    broadcastMessage,
    clientConnect
};
