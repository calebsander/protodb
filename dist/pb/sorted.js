"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(path.join(__dirname, 'sorted.proto'));
exports.LIST_END = 0;
exports.freePageType = protoFile.lookupType('FreePage');
exports.headerType = protoFile.lookupType('Header');
exports.nodeType = protoFile.lookupType('Node');
