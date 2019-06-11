"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const protobuf = require("protobufjs");
const protoFile = protobuf.loadSync(['hash.proto', 'interface.proto'].map(file => path.join(__dirname, file)));
exports.headerType = protoFile.lookupType('Header');
// Header and bucket index have constant size
// for constant-time access into the directory array
exports.HEADER_BYTES = exports.headerType.encode({ depth: 0, size: 0 }).len;
exports.bucketIndexType = protoFile.lookupType('BucketIndex');
exports.BUCKET_INDEX_BYTES = exports.bucketIndexType.encode({ page: 0 }).len;
exports.bucketItemType = protoFile.lookupType('KeyValuePair');
exports.bucketType = protoFile.lookupType('Bucket');
