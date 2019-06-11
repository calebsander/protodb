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
// Obtains a free page from the free list, or by extending the file.
// Not atomic, so only one call should be performed at a time.
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
// Atomically adds a free page to the front of the free list
const addFreePage = (name, header, pageNo) => new cache_1.FilePage(filename(name), pageNo).use(async (page) => {
    const { freePage } = header;
    new Uint8Array(page).set(sorted_1.freePageType.encodeDelimited(freePage).finish());
    freePage.next = pageNo;
});
// Gets the uniquifier of a key
function getUniquifier(key) {
    const [lastElement] = key.slice(-1);
    return lastElement && 'uniquifier' in lastElement
        ? lastElement.uniquifier
        : undefined;
}
// Compares key tuples in lexicographic order
function compareKeys(key1, key2) {
    // Only compare the keys' shared elements
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
            return diff; // if diff is 0, continue onto next element
    }
    return 0;
}
// Finds the index of the key that matches a search key.
// This guides the B+ tree traversal in inner nodes and leaf nodes.
// If the search key exceeds all keys, returns the index after the last key.
function lookupKey(key, keys) {
    let i;
    for (i = 0; i < keys.length; i++) {
        if (compareKeys(keys[i].elements, key) >= 0)
            break;
    }
    return i;
}
// Performs the tree lookup, traversing the path to the leaf
async function lookup(name, { root: page }, key) {
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
        if (!children)
            return path; // at a leaf node, so we are done
        page = children[index]; // at an inner node, so traverse the matched child
    }
}
// Saves a node and any of its ancestors that need saving,
// handling any overflows that occur
async function saveWithOverflow(name, key, path, header) {
    // saving stores whether a change in the child requires the parent to be saved
    // newMaxKey stores whether the key being inserted was the child's highest key
    let saving = true, newMaxKey = true;
    do {
        const { page, node, index } = path.pop();
        // Destructure parent
        const [parent] = path.slice(-1);
        const { node: parentNode, index: parentIndex } = parent || { node: undefined, index: undefined };
        let parentKeys, parentChildren;
        if (parentNode) {
            // istanbul ignore if
            if ('leaf' in parentNode)
                throw new Error('Parent is not an inner node?');
            ({ keys: parentKeys, children: parentChildren } = parentNode.inner);
        }
        // Update key if the child has a new maximum key
        if (newMaxKey) {
            newMaxKey = false;
            // If the child is the rightmost child of its parent inner node,
            // or the child is the root node, there is no maximum key to update
            if (parent && parentIndex < parentKeys.length) {
                const children = 'leaf' in node ? node.leaf.values : node.inner.children;
                // Check whether the maximum element in the child was changed
                if (index === children.length - 1) {
                    parentKeys[parentIndex] = { elements: key };
                    newMaxKey = true;
                }
            }
        }
        try {
            await setNode(name, page, node);
            // Saved node without overflowing
            saving = newMaxKey; // save can stop unless this is a new maximum key
        }
        catch (e) {
            // Node overflowed
            util_1.ensureOverflowError(e);
            // Get a new page to store the right half of this node
            const newPage = await getFreePage(name, header);
            let newNode; // the new node
            let promotedKey; // the key to split the nodes
            // TODO: this doesn't split leaves evenly
            if ('leaf' in node) { // splitting a leaf
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
                [promotedKey] = keys.slice(-1); // promote the maximum key from the left
                leaf.next = newPage;
            }
            else { // splitting an inner node
                const { keys, children } = node.inner;
                // istanbul ignore if
                if (keys.length < 2)
                    throw new Error('Item is too large to store');
                const splitIndex = (keys.length >> 1) + 1;
                newNode = { inner: {
                        keys: keys.splice(splitIndex),
                        children: children.splice(splitIndex)
                    } };
                // Promote the dangling maximum key from the left
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
                    await setNode(name, rootPage, {
                        inner: { keys: [promotedKey], children: [page, newPage] }
                    });
                })());
                saving = false; // there is no parent to save
            }
            await Promise.all(promises);
        }
    } while (saving); // continue until the save stops propagating
}
// Tries to coalesce a node with its siblings; returns whether successful
async function tryCoalesce(name, node, path, header) {
    if (!path.length)
        return false; // root node can't be coalesced
    const { len } = sorted_1.nodeType.encodeDelimited(node);
    if (len >= MIN_NODE_LENGTH)
        return false; // ensure node is sufficiently empty
    // Find possible siblings to coalesce with
    const [{ node: parentNode, index }] = path.slice(-1);
    // istanbul ignore if
    if ('leaf' in parentNode)
        throw new Error('Parent is not a leaf?');
    const { keys, children } = parentNode.inner;
    let thisPage = children[index]; // the page storing the current node
    const file = filename(name);
    const siblingLengths = await Promise.all([true, false]
        .map(left => {
        const siblingIndex = left ? index - 1 : index + 1;
        return { left, siblingIndex, siblingPage: children[siblingIndex] };
    })
        .filter(({ siblingPage }) => siblingPage) // skip siblings that don't exist
        .map(async (sibling) => {
        const length = await util_1.getNodeLength(file, sibling.siblingPage);
        return { sibling, length };
    }));
    const originalNode = node;
    const newFreePages = [];
    // Coalescing is only possible with less than half-full siblings
    const coalesceCandidates = siblingLengths.filter(({ length }) => length < MIN_NODE_LENGTH);
    while (coalesceCandidates.length) {
        // Choose the smaller of the candidate siblings to coalesce
        const coalesceSibling = util_1.argmin(coalesceCandidates, ({ length }) => length);
        // Coalesce with selected sibling
        const [{ sibling }] = coalesceCandidates.splice(coalesceSibling, 1);
        const { left, siblingIndex, siblingPage } = sibling;
        const siblingNode = await getNode(name, siblingPage);
        // We always coalesce into the left sibling so that if it is a leaf,
        // we don't have to change the "next" value of the previous leaf
        let leftNode, rightNode;
        let leftIndex = siblingIndex;
        if (left) {
            leftNode = siblingNode;
            rightNode = node;
            newFreePages.push(thisPage);
            thisPage = siblingPage; // the left sibling is the one that survives
        }
        else {
            leftNode = node;
            rightNode = siblingNode;
            leftIndex--; // siblingIndex is to the right, one index too high
            newFreePages.push(siblingPage);
        }
        // Remove key between the siblings and the right sibling's page from parent
        const [splitKey] = keys.splice(leftIndex, 1);
        children.splice(leftIndex + 1, 1);
        let newSize; // don't calculate size unless it's needed
        if ('leaf' in leftNode) { // coalescing leaf nodes
            // istanbul ignore if
            if ('inner' in rightNode)
                throw new Error('Invalid sibling?');
            const leftLeaf = leftNode.leaf, rightLeaf = rightNode.leaf;
            const values = leftLeaf.values.slice();
            // Copy right node's values because they are slices of its page,
            // which will be overwritten when it gets added to the free list
            for (const value of rightLeaf.values)
                values.push(value.slice());
            // Combine the keys and values
            node = { leaf: {
                    keys: [...leftLeaf.keys, ...rightLeaf.keys],
                    values,
                    next: rightLeaf.next
                } };
        }
        else { // coalescing inner nodes
            // istanbul ignore if
            if ('leaf' in rightNode)
                throw new Error('Invalid sibling?');
            const leftInner = leftNode.inner, rightInner = rightNode.inner;
            // Combine the nodes, demote the key between them
            const newNode = { inner: {
                    keys: [...leftInner.keys, splitKey, ...rightInner.keys],
                    children: [...leftInner.children, ...rightInner.children]
                } };
            // Ensure that this node is not too big (since it includes the split key)
            newSize = sorted_1.nodeType.encodeDelimited(newNode).len;
            if (newSize > cache_1.PAGE_SIZE)
                break;
            node = newNode;
        }
        // See if it is possible to coalesce with the other sibling
        if (coalesceCandidates.length) {
            newSize = newSize || sorted_1.nodeType.encodeDelimited(node).len;
            if (newSize < MIN_NODE_LENGTH) {
                // The index of the right sibling goes down if the left sibling is removed
                if (left)
                    coalesceCandidates[0].sibling.siblingIndex--;
            }
            else
                break;
        }
    }
    if (node === originalNode)
        return false; // no coalescing occurred
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
// Generates the key-value pairs in a given key range
async function* pairsFrom(name, inclusive, start, end) {
    const path = await lookup(name, await getHeader(name), start);
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
        // Follow linked list of leaves to avoid retraversing the tree
        node = await getNode(name, next);
        index = 0;
    }
}
const iterators = new iterator_1.Iterators();
async function create(name) {
    await _1.addCollection(name, interface_1.CollectionType.SORTED);
    const file = filename(name);
    await cache_1.createFile(file);
    await cache_1.setPageCount(file, 2); // allocate the header page and root node
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
    const header = await getHeader(name);
    const path = await lookup(name, header, searchKey);
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
    // Remove key-value pair from leaf and save all changed nodes along path
    keys.splice(index, 1);
    values.splice(index, 1);
    let coalesced;
    do {
        const { page, node } = path.pop();
        // Only coalesce if child was coalesced
        coalesced = await tryCoalesce(name, node, path, header);
        // If node couldn't be coalesced, save it as-is
        if (!coalesced)
            await setNode(name, page, node);
    } while (path.length && coalesced);
    header.size--;
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
    const header = await getHeader(name);
    const path = await lookup(name, header, key);
    const [{ node, index }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    const { keys, values } = node.leaf;
    // If the key is already there, add uniquifiers
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
    // Insert key-value pair and save nodes along path
    keys.splice(index, 0, { elements: key });
    values.splice(index, 0, value);
    await saveWithOverflow(name, key, path, header);
    header.size++;
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
    // ">= []" will match all keys
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
