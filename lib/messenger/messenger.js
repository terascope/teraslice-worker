'use strict';

const set = require('lodash/set');
const cloneDeep = require('lodash/cloneDeep');
const { EventEmitter } = require('events');
const { newId } = require('../utils');

class Messenger extends EventEmitter {
    constructor(opts, type) {
        super();

        this.networkerLatencyBuffer = opts.networkerLatencyBuffer || 0;
        this.actionTimeout = opts.actionTimeout;
        this.type = type;
    }

    close() {
        this.closed = true;
        this.removeAllListeners();
    }

    _broadcast(message) {
        if (this.type === 'server') {
            this.server.sockets.emit(message.message, message);
        }

        if (this.type === 'client') {
            this.socket.emit(message.message, message);
        }
    }

    _send(message) {
        if (this.type === 'server') {
            this.server.sockets.in(message.address).emit(message.message, message);
        }

        if (this.type === 'client') {
            this.socket.emit(message.message, message);
        }
    }

    async _sendWithResponse(msg, timeoutMs, retry) {
        const message = Object.assign({
            __msgId: newId()
        }, msg, {
            response: true
        });

        let shouldRetry = false;

        const _onReconnect = async () => {
            const retryMsg = cloneDeep(message);
            shouldRetry = true;
            set(retryMsg, 'payload.retry', true);
            await this._send(retryMsg);
        };

        const _waitForResponse = async () => {
            const response = await this.onceWithTimeout(message.__msgId, timeoutMs, true);
            if (response == null) {
                if (shouldRetry) {
                    shouldRetry = false;
                    return _waitForResponse();
                }
                throw new Error(`Timeout error while communicating with ${message.address}, with message: ${JSON.stringify(message)}`);
            }

            if (response.error) {
                throw new Error(response.error);
            }

            return response.payload;
        };

        await this._send(message);

        if (retry && this.type === 'client') {
            this.socket.on('reconnect', _onReconnect);
        }

        let response;

        try {
            response = await _waitForResponse();
        } finally {
            if (retry && this.type === 'client') {
                this.socket.removeListener('reconnect', _onReconnect);
            }
        }

        return response;
    }

    respond(msg, payload) {
        const responseMsg = {
            __msgId: msg.__msgId,
            __source: msg.__source,
            address: msg.address,
            message: 'messaging:response',
            payload,
        };
        return this._send(responseMsg);
    }

    async onceWithTimeout(eventName, timeout, skipError) {
        const timeoutMs = this._getTimeout(timeout);

        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.removeListener(eventName, _onceWithTimeout);
                if (skipError) {
                    resolve();
                    return;
                }
                const error = new Error(`Timed out after ${timeoutMs}ms, waiting for event "${eventName}"`);
                error.code = 408;
                reject(error);
            }, timeoutMs);

            function _onceWithTimeout(msg) {
                clearTimeout(timer);
                resolve(msg);
            }

            this.once(eventName, _onceWithTimeout);
        });
    }

    handleResponses(socket) {
        socket.on('messaging:response', (msg) => {
            if (!msg.__msgId) {
                console.error('Messaging response requires an a msgId') // eslint-disable-line
                return;
            }
            this.emit(msg.__msgId, msg);
        });
    }

    // do this to make it easier to listen for a specific messages
    _emit(eventName, msg, keys = []) {
        keys.forEach((key) => {
            if (!key) return;
            this.emit(`${eventName}:${key}`, msg);
        });

        this.emit(`${eventName}`, msg);
    }

    _getTimeout(timeout) {
        return (timeout || this.actionTimeout) + this.networkerLatencyBuffer;
    }
}

module.exports = Messenger;
