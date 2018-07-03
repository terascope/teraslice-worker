'use strict';

const Promise = require('bluebird');
const shortid = require('shortid');

function sendMessageWithResponse(socket, eventName, payload, { timeoutMs, source, to }) {
    const message = {
        __msgId: shortid.generate(),
        __source: source,
        response: true,
        payload
    };
    return new Promise((resolve, reject) => {
        let timeout;
        function _handleResponse(msg) {
            if (msg.__msgId === message.__msgId) {
                clearTimeout(timeout);
                socket.removeListener('message:response', _handleResponse);
                if (msg.error) {
                    reject(new Error(msg.error));
                } else {
                    resolve(msg.payload);
                }
            }
        }

        socket.on('message:response', _handleResponse);
        socket.emit(eventName, message);

        timeout = setTimeout(() => {
            socket.removeListener('message:response', _handleResponse);
            const error = new Error(`Timeout error while communicating with ${to}, with message: ${JSON.stringify(message)}`);
            reject(error);
        }, timeoutMs);
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

function serverClose(server) {
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
    serverClose,
    onMessage,
    sendMessageWithResponse,
    clientConnect
};
