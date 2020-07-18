"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getNodeLength = exports.ensureOverflowError = exports.argmin = void 0;
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
let OVERFLOW_ERROR_TYPE, OVERFLOW_ERROR_MESSAGE;
try {
    // Trigger an overflow by assigning 1 byte to a 0-byte buffer
    const buffer = new Uint8Array;
    buffer.set([0]);
}
catch (e) {
    const err = e;
    OVERFLOW_ERROR_TYPE = err.constructor;
    OVERFLOW_ERROR_MESSAGE = err.message;
}
// Checks that an error is the result of a write overflowing its page
function ensureOverflowError(e) {
    // istanbul ignore if
    if (!(e instanceof OVERFLOW_ERROR_TYPE && e.message === OVERFLOW_ERROR_MESSAGE)) {
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
