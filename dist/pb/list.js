"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(path.join(__dirname, 'list.proto'));
exports.childType = protoFile.lookupType('Child');
// Value of FreePage.next at the end of the free list.
// 0 is the page containing the header, which is always in use.
exports.FREE_LIST_END = 0;
exports.freePageType = protoFile.lookupType('FreePage');
exports.headerType = protoFile.lookupType('Header');
exports.nodeType = protoFile.lookupType('Node');
