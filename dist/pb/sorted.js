"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.nodeType = exports.headerType = exports.freePageType = exports.LIST_END = void 0;
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(['interface.proto', 'sorted.proto'].map(file => path.join(__dirname, file)));
// Value of FreePage.next or LeafNode.next at the end of their lists.
// 0 is the page containing the header, so there is no possibile conflict.
exports.LIST_END = 0;
exports.freePageType = protoFile.lookupType('FreePage');
exports.headerType = protoFile.lookupType('Header');
exports.nodeType = protoFile.lookupType('Node');
