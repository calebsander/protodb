"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const util_1 = require("util");
const constants_1 = require("./constants");
const randomBytesPromise = util_1.promisify(crypto_1.randomBytes);
const getKey = (iter) => Buffer.from(iter.buffer, iter.byteOffset, iter.byteLength).toString('hex');
class Iterators {
    constructor() {
        this.iterators = new Map();
        this.iteratorCounts = new Map();
    }
    async registerIterator(name, iterator) {
        this.iteratorCounts.set(name, (this.iteratorCounts.get(name) || 0) + 1);
        const iter = await randomBytesPromise(constants_1.ITER_BYTE_LENGTH);
        this.iterators.set(getKey(iter), { name, iterator });
        return iter;
    }
    getIterator(iter) {
        const iterator = this.iterators.get(getKey(iter));
        if (!iterator)
            throw new Error('Unknown iterator');
        return iterator.iterator;
    }
    closeIterator(iter) {
        const key = getKey(iter);
        const iterator = this.iterators.get(key);
        if (!iterator)
            throw new Error('Unknown iterator');
        const { name } = iterator;
        this.iterators.delete(getKey(iter));
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
