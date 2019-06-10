"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
exports.itemType = protobuf.loadSync(path.join(__dirname, 'item.proto'))
    .lookupType('Item');
