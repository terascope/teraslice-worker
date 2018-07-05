'use strict';

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
};
