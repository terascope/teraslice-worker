'use strict';

const { EventEmitter } = require('events');
const { newId } = require('../utils');

class MessageEmitter extends EventEmitter {
    close() {
        this.removeAllListeners();
    }

    _send(msg) {
        const message = Object.assign({
            __source: this.source,
        }, msg);

        if (this.source === 'execution_controller') {
            this.server.sockets.in(message.address).emit(message.message, message);
            return;
        }
        if (this.source === 'worker') {
            this.socket.emit(message.message, message);
            return;
        }

        throw new Error(`Unable to send message with source of ${this.source}`);
    }

    async _sendWithResponse(msg, timeoutMs) {
        const message = Object.assign({
            __msgId: newId()
        }, msg, {
            response: true
        });

        await this._send(message);

        let response;
        try {
            response = await this.onceWithTimeout(message.__msgId, timeoutMs);
        } catch (err) {
            throw new Error(`Timeout error while communicating with ${message.address}, with message: ${JSON.stringify(message)}`);
        }

        if (response.error) {
            throw new Error(response.error);
        }

        return response.payload;
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

    async onceWithTimeout(eventName, timeout) {
        const timeoutMs = this._getTimeout(timeout);
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.removeListener(eventName, _onceWithTimeout);
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

module.exports = MessageEmitter;
