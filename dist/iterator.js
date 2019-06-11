"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const util_1 = require("util");
const constants_1 = require("./constants");
const randomBytesPromise = util_1.promisify(crypto_1.randomBytes);
// Converts a 16-byte iterator handle to a string for use as a Map key
const getKey = (iter) => Buffer.from(iter.buffer, iter.byteOffset, iter.length).toString('hex');
class Iterators {
    constructor() {
        // Maps iterator handles to their associated iterators
        this.iterators = new Map();
        // Maps collection names to their number of active iterators
        this.iteratorCounts = new Map();
    }
    lookupIterator(key) {
        const iterator = this.iterators.get(key);
        if (!iterator)
            throw new Error('Unknown iterator');
        return iterator;
    }
    async registerIterator(name, iterator) {
        const iter = await randomBytesPromise(constants_1.ITER_BYTE_LENGTH);
        this.iterators.set(getKey(iter), { name, iterator });
        this.iteratorCounts.set(name, (this.iteratorCounts.get(name) || 0) + 1);
        return iter;
    }
    getIterator(iter) {
        return this.lookupIterator(getKey(iter)).iterator;
    }
    closeIterator(iter) {
        const key = getKey(iter);
        const { name } = this.lookupIterator(key);
        this.iterators.delete(key);
        const oldCount = this.iteratorCounts.get(name);
        // istanbul ignore if
        if (!oldCount)
            throw new Error('Hash has no iterators?');
        if (oldCount > 1)
            this.iteratorCounts.set(name, oldCount - 1);
        else
            this.iteratorCounts.delete(name);
    }
    checkNoIterators(name) {
        if (this.iteratorCounts.has(name)) {
            throw new Error(`Collection ${name} has active iterators`);
        }
    }
}
exports.Iterators = Iterators;
