"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const _1 = require(".");
const args_1 = require("../args");
const cache_1 = require("../cache");
const iterator_1 = require("../iterator");
const interface_1 = require("../pb/interface");
const sorted_1 = require("../pb/sorted");
const util_1 = require("../util");
const HEADER_PAGE = 0;
const INITIAL_ROOT_PAGE = 1;
// Slightly less than half because joining inner nodes requires
// pulling down the split key from their parent
const MIN_NODE_LENGTH = (cache_1.PAGE_SIZE * 0.45) | 0;
const filename = (name) => path.join(args_1.dataDir, `${name}.sorted`);
async function checkIsSorted(name) {
    const collections = await _1.getCollections;
    const collection = collections[name];
    if (collection !== interface_1.CollectionType.SORTED) {
        throw new Error(`Collection ${name} is not a sorted map`);
    }
}
const getHeader = (name) => new cache_1.FilePage(filename(name), HEADER_PAGE).use(async (page) => sorted_1.headerType.toObject(sorted_1.headerType.decodeDelimited(new Uint8Array(page)), { longs: Number }));
const setHeader = (name, header) => new cache_1.FilePage(filename(name), HEADER_PAGE).use(async (page) => new Uint8Array(page).set(sorted_1.headerType.encodeDelimited(header).finish()));
const getNode = (name, page) => new cache_1.FilePage(filename(name), page).use(async (page) => sorted_1.nodeType.toObject(sorted_1.nodeType.decodeDelimited(new Uint8Array(page)), { defaults: true, longs: Number }));
const setNode = (name, page, node) => new cache_1.FilePage(filename(name), page).use(async (page) => new Uint8Array(page).set(sorted_1.nodeType.encodeDelimited(node).finish()));
async function getFreePage(name, header) {
    const file = filename(name);
    const freePage = header.freePage.next;
    if (freePage === sorted_1.LIST_END) {
        const pages = await cache_1.getPageCount(file);
        await cache_1.setPageCount(file, pages + 1);
        return pages;
    }
    else {
        header.freePage = await new cache_1.FilePage(file, freePage).use(async (page) => sorted_1.freePageType.toObject(sorted_1.freePageType.decodeDelimited(new Uint8Array(page))));
        return freePage;
    }
}
const addFreePage = (name, header, pageNo) => new cache_1.FilePage(filename(name), pageNo).use(async (page) => {
    const { freePage } = header;
    new Uint8Array(page).set(sorted_1.freePageType.encodeDelimited(freePage).finish());
    freePage.next = pageNo;
});
function getUniquifier(key) {
    const [lastElement] = key.slice(-1);
    return lastElement && 'uniquifier' in lastElement
        ? lastElement.uniquifier
        : undefined;
}
function compareKeys(key1, key2) {
    const minLength = Math.min(key1.length, key2.length);
    for (let i = 0; i < minLength; i++) {
        const element1 = key1[i], element2 = key2[i];
        let diff;
        if ('int' in element1) {
            if (!('int' in element2))
                throw new Error('Key types do not match');
            diff = element1.int - element2.int;
        }
        else if ('float' in element1) {
            if (!('float' in element2))
                throw new Error('Key types do not match');
            diff = element1.float - element2.float;
        }
        else if ('string' in element1) {
            if (!('string' in element2))
                throw new Error('Key types do not match');
            const string1 = element1.string, string2 = element2.string;
            diff = string1 < string2 ? -1 : string1 > string2 ? +1 : 0;
        }
        else {
            if (!('uniquifier' in element2))
                throw new Error('Key types do not match');
            // Uniquifier order is reversed so the leftmost equivalent key
            // has the largest uniquifier
            diff = element2.uniquifier - element1.uniquifier;
        }
        if (diff)
            return diff;
    }
    return 0;
}
function lookupKey(key, keys) {
    const { length } = keys;
    let i;
    for (i = 0; i < length; i++) {
        const comparison = compareKeys(keys[i].elements, key);
        if (comparison >= 0)
            break;
    }
    return i;
}
async function lookup(name, key) {
    let { root: page } = await getHeader(name);
    const path = [];
    while (true) {
        const node = await getNode(name, page);
        let keys;
        let children;
        if ('leaf' in node)
            ({ keys } = node.leaf);
        else
            ({ keys, children } = node.inner);
        const index = lookupKey(key, keys);
        path.push({ page, node, index });
        if (children)
            page = children[index];
        else
            break;
    }
    return path;
}
async function saveWithOverflow(name, key, path, header) {
    let saving = true, newMaxKey = true;
    do {
        const { page, node, index } = path.pop();
        const [parent] = path.slice(-1);
        const { node: parentNode, index: parentIndex } = parent
            ? parent
            : { node: undefined, index: undefined };
        let parentKeys, parentChildren;
        if (parentNode) {
            // istanbul ignore if
            if ('leaf' in parentNode)
                throw new Error('Parent is not an inner node?');
            ({ keys: parentKeys, children: parentChildren } = parentNode.inner);
        }
        if (newMaxKey) {
            newMaxKey = false;
            if (parent) {
                const children = 'leaf' in node ? node.leaf.values : node.inner.children;
                if (index === children.length - 1) { // changing the maximum element
                    if (parentIndex < parentKeys.length) {
                        parentKeys[parentIndex] = { elements: key };
                        newMaxKey = true;
                    }
                }
            }
        }
        try {
            await setNode(name, page, node);
            // Saved node without overflowing
            saving = newMaxKey;
        }
        catch (e) {
            // Node overflowed
            util_1.ensureOverflowError(e);
            const newPage = await getFreePage(name, header);
            let newNode;
            let promotedKey;
            // TODO: this doesn't split leaves evenly
            if ('leaf' in node) {
                const { leaf } = node;
                const { keys, values, next } = leaf;
                const splitIndex = keys.length >> 1;
                // istanbul ignore if
                if (!splitIndex)
                    throw new Error('Item is too large to store');
                newNode = { leaf: {
                        keys: keys.splice(splitIndex),
                        // Make copies of values since they are slices of the old page,
                        // which will be overwritten
                        values: values.splice(splitIndex).map(value => value.slice()),
                        next
                    } };
                [promotedKey] = keys.slice(-1);
                leaf.next = newPage;
            }
            else {
                const { keys, children } = node.inner;
                // istanbul ignore if
                if (keys.length < 2)
                    throw new Error('Item is too large to store');
                const splitIndex = (keys.length >> 1) + 1;
                newNode = { inner: {
                        keys: keys.splice(splitIndex),
                        children: children.splice(splitIndex)
                    } };
                promotedKey = keys.pop();
            }
            const promises = [
                setNode(name, page, node),
                setNode(name, newPage, newNode)
            ];
            // Promote the new key and page to the parent node
            if (parent) {
                parentKeys.splice(parentIndex, 0, promotedKey);
                parentChildren.splice(parentIndex + 1, 0, newPage);
            }
            else { // splitting the root node
                promises.push((async () => {
                    const rootPage = await getFreePage(name, header);
                    header.root = rootPage;
                    await setNode(name, rootPage, { inner: {
                            keys: [promotedKey],
                            children: [page, newPage]
                        } });
                })());
            }
            await Promise.all(promises);
        }
    } while (path.length && saving);
}
async function tryCoalesce(name, node, path, header) {
    if (!path.length)
        return false; // root node can't be coalesced
    const { len } = sorted_1.nodeType.encodeDelimited(node);
    if (len >= MIN_NODE_LENGTH)
        return false; // ensure node is sufficiently empty
    const [{ node: parentNode, index }] = path.slice(-1);
    // istanbul ignore if
    if ('leaf' in parentNode)
        throw new Error('Parent is not a leaf?');
    const { keys, children } = parentNode.inner;
    let thisPage = children[index];
    const file = filename(name);
    const siblingLengths = await Promise.all([true, false]
        .map(left => {
        const siblingIndex = left ? index - 1 : index + 1;
        return { left, siblingIndex, sibling: children[siblingIndex] };
    })
        .filter(({ sibling }) => sibling) // skip siblings that don't exist
        .map(async (sibling) => {
        const length = await util_1.getNodeLength(file, sibling.sibling);
        return { sibling, length };
    }));
    // Coalescing is only possible with less than half-full siblings
    const coalesceCandidates = siblingLengths.filter(({ length }) => length < MIN_NODE_LENGTH);
    if (!coalesceCandidates.length)
        return false;
    const originalNode = node;
    const newFreePages = [];
    let coalesceAgain;
    do {
        // Choose the smaller of the candidate siblings to coalesce
        const coalesceSibling = util_1.argmin(coalesceCandidates, ({ length }) => length);
        // Coalesce with selected sibling
        const [{ sibling }] = coalesceCandidates.splice(coalesceSibling, 1);
        const { left, siblingIndex, sibling: siblingPage } = sibling;
        const siblingNode = await getNode(name, siblingPage);
        // We always coalesce into the left sibling so that if it is a leaf,
        // we don't have to change the "next" value of the previous leaf
        let leftNode, rightNode;
        let leftIndex = siblingIndex;
        if (left) {
            leftNode = siblingNode;
            rightNode = node;
            newFreePages.push(thisPage);
            thisPage = siblingPage;
        }
        else {
            leftNode = node;
            rightNode = siblingNode;
            leftIndex--; // siblingIndex is to the right, one index too high
            newFreePages.push(siblingPage);
        }
        let newSize;
        if ('inner' in leftNode) {
            // istanbul ignore if
            if ('leaf' in rightNode)
                throw new Error('Invalid sibling?');
            const leftInner = leftNode.inner, rightInner = rightNode.inner;
            let newNode = { inner: {
                    keys: [...leftInner.keys, keys[leftIndex], ...rightInner.keys],
                    children: [...leftInner.children, ...rightInner.children]
                } };
            // Ensure that this node is not too big (since it includes the split key)
            newSize = sorted_1.nodeType.encodeDelimited(newNode).len;
            if (newSize > cache_1.PAGE_SIZE)
                break;
            node = newNode;
        }
        else {
            // istanbul ignore if
            if ('inner' in rightNode)
                throw new Error('Invalid sibling?');
            const leftLeaf = leftNode.leaf, rightLeaf = rightNode.leaf;
            const values = leftLeaf.values.slice();
            // Copy right node's values because they are slices of its page,
            // which will be overwritten when it gets added to the free list
            for (const value of rightLeaf.values)
                values.push(value.slice());
            node = { leaf: {
                    keys: [...leftLeaf.keys, ...rightLeaf.keys],
                    values,
                    next: rightLeaf.next
                } };
        }
        // Remove left sibling's key and right sibling's page from parent
        keys.splice(leftIndex, 1);
        children.splice(leftIndex + 1, 1);
        // See if it is possible to coalesce with the other sibling
        if (coalesceCandidates.length) {
            if (newSize === undefined)
                newSize = sorted_1.nodeType.encodeDelimited(node).len;
            coalesceAgain = newSize < MIN_NODE_LENGTH;
            if (coalesceAgain) {
                // The index of the right sibling goes down if the left sibling is removed
                if (left)
                    coalesceCandidates[0].sibling.siblingIndex--;
            }
        }
        else
            coalesceAgain = false;
    } while (coalesceAgain);
    if (node === originalNode)
        return false;
    // Make this the new root if it is the only child of the root node
    if (path.length === 1 && children.length === 1) {
        path.pop();
        newFreePages.push(header.root);
        header.root = thisPage;
    }
    const promises = newFreePages.map(page => addFreePage(name, header, page));
    promises.push(setNode(name, thisPage, node));
    await Promise.all(promises);
    return true;
}
async function* pairsFrom(name, inclusive, start, end) {
    const path = await lookup(name, start);
    let [{ node, index }] = path.slice(-1);
    while (true) {
        // istanbul ignore if
        if ('inner' in node)
            throw new Error('Not a leaf?');
        const { keys, values, next } = node.leaf;
        while (index < keys.length) {
            const key = keys[index].elements;
            if (end) {
                const comparison = compareKeys(key, end);
                // If key is past end, or key is equal to end and the end is exclusive
                if (comparison > 0 || !(inclusive || comparison))
                    return;
            }
            yield { key, value: values[index] };
            index++;
        }
        if (next === sorted_1.LIST_END)
            break;
        node = await getNode(name, next);
        index = 0;
    }
}
const iterators = new iterator_1.Iterators();
async function create(name) {
    await _1.addCollection(name, interface_1.CollectionType.SORTED);
    const file = filename(name);
    await cache_1.createFile(file);
    await cache_1.setPageCount(file, 2);
    await Promise.all([
        setHeader(name, {
            root: INITIAL_ROOT_PAGE,
            size: 0,
            freePage: { next: sorted_1.LIST_END }
        }),
        setNode(name, INITIAL_ROOT_PAGE, {
            leaf: { keys: [], values: [], next: sorted_1.LIST_END }
        })
    ]);
}
exports.create = create;
async function drop(name) {
    await checkIsSorted(name);
    iterators.checkNoIterators(name);
    await Promise.all([_1.dropCollection(name), cache_1.removeFile(filename(name))]);
}
exports.drop = drop;
async function remove(name, searchKey) {
    await checkIsSorted(name);
    iterators.checkNoIterators(name);
    const path = await lookup(name, searchKey);
    const [{ node, index }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    const { keys, values } = node.leaf;
    const oldKey = keys[index];
    // If key doesn't match, deletion can't be performed
    if (!oldKey || compareKeys(oldKey.elements, searchKey)) {
        throw new Error('No matching key');
    }
    keys.splice(index, 1);
    values.splice(index, 1);
    const header = await getHeader(name);
    header.size--;
    let coalesced;
    do {
        const { page, node } = path.pop();
        // Only coalesce if child was coalesced
        coalesced = await tryCoalesce(name, node, path, header);
        // If node couldn't be coalesced, save it as-is
        if (!coalesced)
            await setNode(name, page, node);
    } while (path.length && coalesced);
    await setHeader(name, header);
}
exports.remove = remove;
async function get(name, searchKey) {
    await checkIsSorted(name);
    const pairs = [];
    for await (const pair of pairsFrom(name, true, searchKey, searchKey)) {
        pairs.push(pair);
    }
    return pairs;
}
exports.get = get;
async function insert(name, key, value) {
    if (key.some(element => 'uniquifier' in element)) {
        throw new Error('Key cannot include uniquifier');
    }
    await checkIsSorted(name);
    iterators.checkNoIterators(name);
    const path = await lookup(name, key);
    const [{ node, index }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    const { keys, values } = node.leaf;
    const oldKey = keys[index];
    if (oldKey) {
        const { elements } = oldKey;
        if (!compareKeys(key, elements)) {
            let oldUniquifier = getUniquifier(elements);
            if (oldUniquifier === undefined) {
                oldUniquifier = 0;
                elements.push({ uniquifier: oldUniquifier });
            }
            key.push({ uniquifier: oldUniquifier + 1 });
        }
    }
    keys.splice(index, 0, { elements: key });
    values.splice(index, 0, value);
    const header = await getHeader(name);
    header.size++;
    await saveWithOverflow(name, key, path, header);
    await setHeader(name, header);
}
exports.insert = insert;
async function size(name) {
    await checkIsSorted(name);
    const { size } = await getHeader(name);
    return size;
}
exports.size = size;
async function iter(name, inclusive, start, end) {
    await checkIsSorted(name);
    const iterator = pairsFrom(name, inclusive, start || [], end);
    return iterators.registerIterator(name, iterator);
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
