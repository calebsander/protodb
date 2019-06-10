#!/usr/bin/env node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = require("fs");
const net = require("net");
const args_1 = require("./args");
const cache_1 = require("./cache");
const command_processor_1 = require("./command-processor");
const util_1 = require("./util");
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
    net.createServer({ allowHalfOpen: true }, connection => {
        const chunks = [];
        connection
            .on('data', chunk => chunks.push(chunk))
            .on('end', async () => connection.end(await command_processor_1.executeCommand(util_1.concat(chunks))));
    }).listen(args_1.port);
    console.log(`Listening on port ${args_1.port}`);
})();
