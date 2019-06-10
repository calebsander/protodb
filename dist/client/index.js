"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const net = require("net");
const constants_1 = require("../constants");
const request_1 = require("../pb/request");
const util_1 = require("../util");
const toUint8Array = (buffer) => buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
const toOptionalIndex = (index) => index === undefined ? { none: {} } : { value: index };
const toOptionalKey = (key) => key ? { value: { elements: key } } : { none: {} };
const toOptionalBytes = (value) => 'data' in value ? value.data : null;
class ProtoDBError extends Error {
    get name() {
        return this.constructor.name;
    }
}
exports.ProtoDBError = ProtoDBError;
class ProtoDBClient {
    constructor(port = constants_1.DEFAULT_PORT, host = 'localhost') {
        this.port = port;
        this.host = host;
    }
    async runCommand(command, responseType) {
        const client = net.connect(this.port, this.host, () => client.end(request_1.commandType.encode(command).finish()));
        const data = await new Promise((resolve, reject) => {
            const chunks = [];
            client
                .on('data', chunk => chunks.push(chunk))
                .on('end', () => resolve(util_1.concat(chunks)))
                .on('error', reject);
        });
        const response = responseType.toObject(responseType.decode(data), { defaults: true, longs: Number });
        if ('error' in response)
            throw new ProtoDBError(response.error);
        return response;
    }
    async list() {
        const { db } = await this.runCommand({ list: {} }, request_1.listResponseType);
        return db;
    }
    async itemCreate(name) {
        await this.runCommand({ itemCreate: { name } }, request_1.voidResponseType);
    }
    async itemDrop(name) {
        await this.runCommand({ itemDrop: { name } }, request_1.voidResponseType);
    }
    async itemGet(name) {
        const { data } = await this.runCommand({ itemGet: { name } }, request_1.bytesResponseType);
        return data;
    }
    async itemSet(name, value) {
        await this.runCommand({ itemSet: { name, value: toUint8Array(value) } }, request_1.voidResponseType);
    }
    async hashCreate(name) {
        await this.runCommand({ hashCreate: { name } }, request_1.voidResponseType);
    }
    async hashDrop(name) {
        await this.runCommand({ hashDrop: { name } }, request_1.voidResponseType);
    }
    async hashDelete(name, key) {
        await this.runCommand({ hashDelete: { name, key: toUint8Array(key) } }, request_1.optionalBytesResponseType);
    }
    async hashGet(name, key) {
        const value = await this.runCommand({ hashGet: { name, key: toUint8Array(key) } }, request_1.optionalBytesResponseType);
        return toOptionalBytes(value);
    }
    async hashSet(name, key, value) {
        await this.runCommand({ hashSet: { name, key: toUint8Array(key), value: toUint8Array(value) } }, request_1.voidResponseType);
    }
    async hashSize(name) {
        const { size } = await this.runCommand({ hashSize: { name } }, request_1.sizeResponseType);
        return size;
    }
    async hashIter(name) {
        const { iter } = await this.runCommand({ hashIter: { name } }, request_1.iterResponseType);
        return iter;
    }
    async hashIterBreak(iter) {
        await this.runCommand({ hashIterBreak: { iter } }, request_1.voidResponseType);
    }
    async hashIterNext(iter) {
        const { pair } = await this.runCommand({ hashIterNext: { iter } }, request_1.optionalPairResponseType);
        return pair || null;
    }
    async listCreate(name) {
        await this.runCommand({ listCreate: { name } }, request_1.voidResponseType);
    }
    async listDrop(name) {
        await this.runCommand({ listDrop: { name } }, request_1.voidResponseType);
    }
    async listDelete(name, index) {
        await this.runCommand({ listDelete: { name, index: toOptionalIndex(index) } }, request_1.voidResponseType);
    }
    async listGet(name, index) {
        const { data } = await this.runCommand({ listGet: { name, index } }, request_1.bytesResponseType);
        return data;
    }
    async listInsert(name, value, index) {
        await this.runCommand({ listInsert: {
                name,
                index: toOptionalIndex(index),
                value: toUint8Array(value)
            } }, request_1.voidResponseType);
    }
    async listSet(name, index, value) {
        await this.runCommand({ listSet: { name, index, value: toUint8Array(value) } }, request_1.voidResponseType);
    }
    async listSize(name) {
        const { size } = await this.runCommand({ listSize: { name } }, request_1.sizeResponseType);
        return size;
    }
    async listIter(name, start, end) {
        if (start && start < 0 || end && end < 0) {
            throw new RangeError(`Bounds cannot be end-relative; got ${start} and ${end}`);
        }
        const { iter } = await this.runCommand({ listIter: {
                name,
                start: toOptionalIndex(start),
                end: toOptionalIndex(end)
            } }, request_1.iterResponseType);
        return iter;
    }
    async listIterBreak(iter) {
        await this.runCommand({ listIterBreak: { iter } }, request_1.voidResponseType);
    }
    async listIterNext(iter) {
        const value = await this.runCommand({ listIterNext: { iter } }, request_1.optionalBytesResponseType);
        return toOptionalBytes(value);
    }
    async sortedCreate(name) {
        await this.runCommand({ sortedCreate: { name } }, request_1.voidResponseType);
    }
    async sortedDrop(name) {
        await this.runCommand({ sortedDrop: { name } }, request_1.voidResponseType);
    }
    async sortedDelete(name, key) {
        await this.runCommand({ sortedDelete: { name, key } }, request_1.voidResponseType);
    }
    async sortedGet(name, key) {
        const { pairs } = await this.runCommand({ sortedGet: { name, key } }, request_1.sortedPairListResponseType);
        return pairs.pairs;
    }
    async sortedInsert(name, key, value) {
        await this.runCommand({ sortedInsert: { name, key, value: toUint8Array(value) } }, request_1.voidResponseType);
    }
    async sortedSize(name) {
        const { size } = await this.runCommand({ sortedSize: { name } }, request_1.sizeResponseType);
        return size;
    }
    async sortedIter(name, start, end, inclusive = false) {
        const { iter } = await this.runCommand({ sortedIter: {
                name,
                start: toOptionalKey(start),
                end: toOptionalKey(end),
                inclusive
            } }, request_1.iterResponseType);
        return iter;
    }
    async sortedIterBreak(iter) {
        await this.runCommand({ sortedIterBreak: { iter } }, request_1.voidResponseType);
    }
    async sortedIterNext(iter) {
        const { pair } = await this.runCommand({ sortedIterNext: { iter } }, request_1.optionalSortedPairResponse);
        return pair || null;
    }
}
exports.ProtoDBClient = ProtoDBClient;
