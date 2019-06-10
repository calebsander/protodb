"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const protobufjs_1 = require("protobufjs");
const cache_1 = require("./cache");
function argmin(arr, keyFunc) {
    let minIndex;
    let minValue = Infinity;
    arr.forEach((item, index) => {
        const value = keyFunc(item);
        if (value < minValue) {
            minIndex = index;
            minValue = value;
        }
    });
    // istanbul ignore if
    if (minIndex === undefined)
        throw new Error('Empty array');
    return minIndex;
}
exports.argmin = argmin;
function concat(buffers) {
    const totalLength = buffers.reduce((totalLength, { length }) => totalLength + length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const buffer of buffers) {
        result.set(buffer, offset);
        offset += buffer.length;
    }
    return result;
}
exports.concat = concat;
function ensureOverflowError(e) {
    // istanbul ignore if
    if (!(e instanceof RangeError && e.message === 'Source is too large')) {
        throw e; // unexpected error; rethrow it
    }
}
exports.ensureOverflowError = ensureOverflowError;
exports.getNodeLength = (file, page) => new cache_1.FilePage(file, page).use(async (page) => {
    const reader = new protobufjs_1.Reader(new Uint8Array(page));
    return reader.uint32() + reader.pos;
});
