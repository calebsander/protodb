"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(['request.proto', 'db.proto', 'sorted.proto']
    .map(file => path.join(__dirname, file)));
exports.commandType = protoFile.lookupType('Command');
exports.bytesResponseType = protoFile.lookupType('BytesResponse');
exports.iterResponseType = protoFile.lookupType('IterResponse');
exports.listResponseType = protoFile.lookupType('ListResponse');
exports.optionalBytesResponseType = protoFile.lookupType('OptionalBytesResponse');
exports.optionalPairResponseType = protoFile.lookupType('OptionalPairResponse');
exports.optionalSortedPairResponse = protoFile.lookupType('OptionalSortedPairResponse');
exports.sizeResponseType = protoFile.lookupType('SizeResponse');
exports.sortedPairListResponseType = protoFile.lookupType('SortedPairListResponse');
exports.voidResponseType = protoFile.lookupType('VoidResponse');
