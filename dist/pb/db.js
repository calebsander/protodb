"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
exports.dbType = protobuf.loadSync(path.join(__dirname, 'db.proto'))
    .lookupType('DB');
