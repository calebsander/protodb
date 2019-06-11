"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(path.join(__dirname, 'hash.proto'));
exports.headerType = protoFile.lookupType('Header');
exports.HEADER_BYTES = exports.headerType.encode({ depth: 0, size: 0 }).finish().length;
exports.bucketIndexType = protoFile.lookupType('BucketIndex');
exports.BUCKET_INDEX_BYTES = exports.bucketIndexType.encode({ page: 0 }).finish().length;
exports.bucketItemType = protoFile.lookupType('BucketItem');
exports.bucketType = protoFile.lookupType('Bucket');