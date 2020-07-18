"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.itemType = void 0;
const path = require("path");
const protobuf = require("protobufjs");
exports.itemType = protobuf.loadSync(path.join(__dirname, 'item.proto'))
    .lookupType('Item');
