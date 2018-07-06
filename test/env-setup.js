'use strict';

const ElasticsearchClient = require('elasticsearch').Client;

process.env.TERAFOUNDATION_CONFIG = '';

module.exports = async () => {
    const es = new ElasticsearchClient({
        host: 'http://localhost:9200',
        log: '' // This suppresses error logging from the ES library.
    });
    await es.indices.delete({ index: 'tmp*' });
};
