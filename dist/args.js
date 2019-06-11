"use strict";
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
const yargs = require("yargs");
const constants_1 = require("./constants");
// Parses the command line options to configure the data directory and TCP port
_a = yargs.options({
    dataDir: {
        alias: 'd',
        default: constants_1.DEFAULT_DATA_DIR,
        describe: 'Directory to store database files',
        string: true
    },
    port: {
        alias: 'p',
        default: constants_1.DEFAULT_PORT,
        describe: 'Port for protoDB to listen on',
        number: true
    }
}).argv, exports.dataDir = _a.dataDir, exports.port = _a.port;
