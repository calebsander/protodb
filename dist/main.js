#!/usr/bin/env node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = require("fs");
const net = require("net");
const args_1 = require("./args");
const cache_1 = require("./cache");
const command_processor_1 = require("./command-processor");
const delimited_stream_1 = require("./delimited-stream");
const initDB = () => fs_1.promises.mkdir(args_1.dataDir).catch(_ => { }); // not a problem if it already exists
async function cleanup() {
    try {
        await cache_1.shutdown();
    }
    catch (e) {
        // istanbul ignore next
        console.error('Shutdown failed with error:', e);
    }
    process.exit();
}
(async () => {
    await initDB();
    process
        .on('exit', code => {
        // istanbul ignore if
        if (code)
            console.error('Cache may not have been flushed');
    })
        .on('SIGTERM', cleanup)
        .on('SIGINT', cleanup);
    net.createServer(connection => {
        const responseStream = new delimited_stream_1.DelimitedWriter;
        responseStream.pipe(connection);
        connection.pipe(new delimited_stream_1.DelimitedReader)
            .on('data', (command) => command_processor_1.processCommand(command, response => responseStream.write(response)));
    }).listen(args_1.port);
    console.log(`Listening on port ${args_1.port}`);
})();
