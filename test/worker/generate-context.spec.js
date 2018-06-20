'use strict';

const { Context } = require('../../');

describe('Terafoundation Context', () => {
    let context;
    beforeEach(() => {
        context = new Context();
    });

    it('should return a limited terafoundation context', () => {
        const expected = {};
        return expect(context.generate()).resolves.toEqual(expected);
    });

    it('should set the proper config', () => {
        expect(context.config).toEqual({
            name: 'teraslice-worker',
            config_schema: Context.configSchema,
            schema_formats: Context.schemaFormats,
            ops_directory: Context.opsDirectory,
            cluster_name: Context.clusterName,
            logging_connection: Context.loggingConnection
        });
    });
});

