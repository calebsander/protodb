"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.dbType = void 0;
const path = require("path");
const protobuf = require("protobufjs");
exports.dbType = protobuf.loadSync(path.join(__dirname, 'interface.proto'))
    .lookupType('DB');
