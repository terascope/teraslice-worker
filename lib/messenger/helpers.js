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

function onMessage(emitter, eventName, timeoutMs = 60000) {
    return new Promise((resolve, reject) => {
        let timer;
        const done = (err, msg) => {
            clearTimeout(timer);
            emitter.removeListener(eventName, _onMessage);
            if (err) {
                reject(err);
                return;
            }
            resolve(msg);
        };
        timer = setTimeout(() => {
            const error = new Error(`Timeout waiting for event "${eventName}"`);
            error.code = 408;
            done(error);
        }, timeoutMs);

        function _onMessage(msg) {
            done(null, msg);
        }

        emitter.once(eventName, _onMessage);
    });
}

module.exports = {
    onMessage,
    emitMessage,
    clientConnect
};
