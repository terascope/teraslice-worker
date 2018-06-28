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

function onMessage(emitter, eventName, timeoutMs = 60000) {
    return new Promise((resolve, reject) => {
        let timer;
        const done = (err, msg) => {
            clearTimeout(timer);
            emitter.removeListener(eventName, done);
            if (err) {
                reject(err);
                return;
            }
            resolve(msg);
        };
        timer = setTimeout(() => {
            done(new Error(`Timeout waiting for event "${eventName}"`));
        }, timeoutMs);

        emitter.once(eventName, (msg) => {
            done(null, msg);
        });
    });
}

module.exports = {
    onMessage,
    emitMessage,
    clientConnect
};
