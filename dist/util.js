"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const protobufjs_1 = require("protobufjs");
const cache_1 = require("./cache");
// Finds the index of the value in an array that minimizes an objective function
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
// Checks that an error is the result of a write overflowing its page
function ensureOverflowError(e) {
    // istanbul ignore if
    if (!(e instanceof RangeError && e.message === 'Source is too large')) {
        throw e; // unexpected error; rethrow it
    }
}
exports.ensureOverflowError = ensureOverflowError;
// Gets the size of an object that is serialized with encodeDelimited().
// Useful for figuring out what portion of a page is occupied.
exports.getNodeLength = (file, page) => new cache_1.FilePage(file, page).use(async (page) => {
    const reader = new protobufjs_1.Reader(new Uint8Array(page));
    return reader.uint32() + reader.pos;
});
