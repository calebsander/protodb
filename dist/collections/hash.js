"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const path = require("path");
const _1 = require(".");
const args_1 = require("../args");
const cache_1 = require("../cache");
const iterator_1 = require("../iterator");
const hash_1 = require("../pb/hash");
const interface_1 = require("../pb/interface");
const util_1 = require("../util");
// Number of bits of hash to consider initially (only 1 bucket)
const INITIAL_DEPTH = 0;
// crypto hashing algorithm to use, chosen because it's the fastest to compute
const HASH = 'sha1';
const filename = (name, fileType) => path.join(args_1.dataDir, `${name}.hash.${fileType}`);
const directoryFilename = (name) => filename(name, 'directory');
const bucketsFilename = (name) => filename(name, 'buckets');
const equal = (buffer1, buffer2) => !Buffer.from(buffer1.buffer, buffer1.byteOffset, buffer1.length)
    .compare(buffer2);
// Computes the unmasked hash of a key
function fullHash(key) {
    const { buffer, byteOffset, length } = crypto_1.createHash(HASH).update(key).digest();
    // Break the hash into 32-bit ints and xor them
    const hash = new Int32Array(buffer, byteOffset, length >> 2);
    let hash32 = 0;
    for (const word of hash)
        hash32 ^= word;
    return hash32;
}
// Computes the bits of the hash used to find the bucket at a given depth
const depthHash = (hash, depth) => hash & ((1 << depth) - 1);
async function checkIsHash(name) {
    const collections = await _1.getCollections;
    const collection = collections[name];
    if (collection !== interface_1.CollectionType.HASH) {
        throw new Error(`Collection ${name} is not a hash`);
    }
}
async function getHeader(name) {
    const contents = await cache_1.getFile(directoryFilename(name), 0, hash_1.HEADER_BYTES);
    return hash_1.headerType.toObject(hash_1.headerType.decode(contents), { longs: Number });
}
function setHeader(name, header) {
    const contents = hash_1.headerType.encode(header).finish();
    return cache_1.setFileSegment(directoryFilename(name), contents, 0, hash_1.HEADER_BYTES);
}
const getBucket = (name, page) => new cache_1.FilePage(bucketsFilename(name), page).use(async (page) => hash_1.bucketType.toObject(hash_1.bucketType.decodeDelimited(new Uint8Array(page)), { defaults: true }));
const setBucket = (name, page, bucket) => new cache_1.FilePage(bucketsFilename(name), page).use(async (page) => new Uint8Array(page).set(hash_1.bucketType.encodeDelimited(bucket).finish()));
async function addBucket(name, page, bucket) {
    await cache_1.setPageCount(bucketsFilename(name), page + 1);
    await setBucket(name, page, bucket);
}
// Gets the position in the directory file of a given bucket index
const locateBucketIndex = (bucket) => hash_1.HEADER_BYTES + bucket * hash_1.BUCKET_INDEX_BYTES;
// Gets the page of the buckets file storing a given bucket index
async function getBucketPage(name, bucket) {
    const offset = locateBucketIndex(bucket);
    const contents = await cache_1.getFile(directoryFilename(name), offset, hash_1.BUCKET_INDEX_BYTES);
    return hash_1.bucketIndexType.toObject(hash_1.bucketIndexType.decode(contents)).page;
}
// Sets a given bucket index to point to a given page of the buckets file
const setBucketPage = (name, bucket, page) => cache_1.setFileSegment(directoryFilename(name), hash_1.bucketIndexType.encode({ page }).finish(), locateBucketIndex(bucket), hash_1.BUCKET_INDEX_BYTES);
// Duplicates the directory, incrementing the depth
async function extendDirectory(name, header) {
    const { depth } = header;
    const indexBytes = hash_1.BUCKET_INDEX_BYTES << depth;
    await cache_1.copyWithinFile(directoryFilename(name), hash_1.HEADER_BYTES, indexBytes, hash_1.HEADER_BYTES + indexBytes);
    header.depth = depth + 1;
}
// Splits a bucket until it no longer overflows a page;
// return whether the global depth changed
async function splitBucket(name, index, bucketPage, { depth, items }, header) {
    // Copy the keys and values because they are slices of the old page,
    // which will be overwritten
    for (const item of items) {
        item.key = item.key.slice();
        item.value = item.value.slice();
    }
    // May need to repeatedly split if keys happen to end up in the same half
    let splitAgain;
    let depthChanged = false;
    do {
        // Grow directory if necessary
        if (depth === header.depth) {
            await extendDirectory(name, header);
            depthChanged = true;
        }
        // Split bucket
        const index1 = index | 1 << depth++;
        const items0 = [], items1 = [];
        for (const item of items) {
            (depthHash(fullHash(item.key), depth) === index ? items0 : items1)
                .push(item);
        }
        splitAgain = false;
        const makeNewBucket = async () => {
            // Add a page for the bucket to the end of the bucket file
            const newBucketPage = await cache_1.getPageCount(bucketsFilename(name));
            const updatePromises = [
                addBucket(name, newBucketPage, { depth, items: items1 })
                    .catch(e => {
                    util_1.ensureOverflowError(e);
                    // istanbul ignore if
                    if (splitAgain)
                        throw new Error('Both buckets overflowed?');
                    splitAgain = true;
                    index = index1;
                    bucketPage = newBucketPage;
                    items = items1;
                })
            ];
            // Update the 2 ** (header.depth - depth) bucket indices
            // that now point to the new bucket
            const bucketRepeatInterval = 1 << depth;
            const maxBucketIndex = 1 << header.depth;
            for (let bucket1 = index1; bucket1 < maxBucketIndex; bucket1 += bucketRepeatInterval) {
                updatePromises.push(setBucketPage(name, bucket1, newBucketPage));
            }
            await Promise.all(updatePromises);
        };
        await Promise.all([
            setBucket(name, bucketPage, { depth, items: items0 })
                .catch(e => {
                util_1.ensureOverflowError(e);
                // istanbul ignore if
                if (splitAgain)
                    throw new Error('Both buckets overflowed?');
                splitAgain = true;
                items = items0;
            }),
            makeNewBucket()
        ]);
    } while (splitAgain);
    return depthChanged;
}
// Generates all the key-value pairs in the hash
async function* hashEntries(name) {
    const buckets = await cache_1.getPageCount(bucketsFilename(name));
    for (let i = 0; i < buckets; i++) {
        const { items } = await getBucket(name, i);
        yield* items;
    }
}
const iterators = new iterator_1.Iterators();
async function create(name) {
    await _1.addCollection(name, interface_1.CollectionType.HASH);
    const initDirectory = async () => {
        const directoryFile = directoryFilename(name);
        await cache_1.createFile(directoryFile);
        await cache_1.setPageCount(directoryFile, 1);
        await Promise.all([
            setHeader(name, { depth: INITIAL_DEPTH, size: 0 }),
            setBucketPage(name, 0, 0)
        ]);
    };
    const initBucket = async () => {
        await cache_1.createFile(bucketsFilename(name));
        await addBucket(name, 0, { depth: INITIAL_DEPTH, items: [] });
    };
    await Promise.all([initDirectory(), initBucket()]);
}
exports.create = create;
async function drop(name) {
    await checkIsHash(name);
    iterators.checkNoIterators(name);
    await Promise.all([
        _1.dropCollection(name),
        cache_1.removeFile(directoryFilename(name)),
        cache_1.removeFile(bucketsFilename(name))
    ]);
}
exports.drop = drop;
// "delete" is a reserved name, so we use "remove" instead
async function remove(name, key) {
    await checkIsHash(name);
    iterators.checkNoIterators(name);
    const header = await getHeader(name);
    const bucketIndex = depthHash(fullHash(key), header.depth);
    const bucketPage = await getBucketPage(name, bucketIndex);
    const bucket = await getBucket(name, bucketPage);
    const { items } = bucket;
    for (let i = 0; i < items.length; i++) {
        if (equal(items[i].key, key)) {
            items.splice(i, 1);
            header.size--;
            await Promise.all([
                setBucket(name, bucketPage, bucket),
                setHeader(name, header)
            ]);
            break;
        }
    }
}
exports.remove = remove;
async function get(name, key) {
    await checkIsHash(name);
    const { depth } = await getHeader(name);
    const bucketIndex = depthHash(fullHash(key), depth);
    const { items } = await getBucket(name, await getBucketPage(name, bucketIndex));
    for (const item of items) {
        if (equal(item.key, key))
            return item.value;
    }
    return null;
}
exports.get = get;
async function set(name, key, value) {
    await checkIsHash(name);
    iterators.checkNoIterators(name);
    const header = await getHeader(name);
    const hash = fullHash(key);
    const bucketIndex = depthHash(hash, header.depth);
    const bucketPage = await getBucketPage(name, bucketIndex);
    const bucket = await getBucket(name, bucketPage);
    // Update value corresponding to key, or add new key-value pair
    const { items } = bucket;
    let newKey = true;
    for (const item of items) {
        if (equal(item.key, key)) {
            item.value = value;
            newKey = false;
            break;
        }
    }
    if (newKey) {
        items.push({ key, value });
        header.size++;
    }
    let depthChanged = false;
    try {
        await setBucket(name, bucketPage, bucket);
    }
    catch (e) { // bucket is full
        util_1.ensureOverflowError(e);
        depthChanged = await splitBucket(name, depthHash(hash, bucket.depth), bucketPage, bucket, header);
    }
    // Only write header if it was modified
    if (newKey || depthChanged)
        await setHeader(name, header);
}
exports.set = set;
async function size(name) {
    await checkIsHash(name);
    const { size } = await getHeader(name);
    return size;
}
exports.size = size;
async function iter(name) {
    await checkIsHash(name);
    return iterators.registerIterator(name, hashEntries(name));
}
exports.iter = iter;
exports.iterBreak = (iter) => iterators.closeIterator(iter);
async function iterNext(iter) {
    const iterator = iterators.getIterator(iter);
    const { value, done } = await iterator.next();
    if (done) {
        iterators.closeIterator(iter);
        return null;
    }
    return value;
}
exports.iterNext = iterNext;
