'use strict';

const { EventEmitter } = require('events');

class MessageEmitter extends EventEmitter {
    constructor() {
        super();
        this._cache = {};
    }

    emit(eventName, msg) {
        if (!this._hasListeners(eventName)) {
            if (this._cache[eventName]) {
                this._cache[eventName].push(msg);
            } else {
                this._cache[eventName] = [msg];
            }
        }
        super.emit(eventName, msg);
    }

    on(eventName, fn) {
        const results = this._cache[eventName] || [];
        while (results.length) {
            fn(results.shift());
        }

        super.on(eventName, fn);
    }

    once(eventName, fn) {
        const results = this._cache[eventName] || [];
        if (results.length) {
            fn(results.shift());
            return;
        }

        super.once(eventName, fn);
    }

    onceWithTimeout(eventName, timeout) {
        const timeoutMs = this._getTimeout(timeout);
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.removeListener(eventName, _onMessage);
                const error = new Error(`Timed out after ${timeoutMs}ms, waiting for event "${eventName}"`);
                error.code = 408;
                reject(error);
            }, timeoutMs);

            function _onMessage(msg) {
                clearTimeout(timer);
                resolve(msg);
            }

            this.once(eventName, _onMessage);
        });
    }

    // flush cache with any key
    flushAny(key) {
        Object.keys(this._cache).forEach((eventName) => {
            if (eventName.indexOf(key) > -1) {
                this._cache[eventName].length = 0;
            }
        });
    }

    flushAll() {
        Object.keys(this._cache).forEach((eventName) => {
            this._cache[eventName].length = 0;
        });
    }

    _hasListeners(eventName) {
        return this.listeners(eventName).length;
    }

    _getTimeout(timeout) {
        return (timeout || this.actionTimeout) + this.networkerLatencyBuffer;
    }
}

module.exports = MessageEmitter;
