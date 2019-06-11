"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const _1 = require(".");
const args_1 = require("../args");
const cache_1 = require("../cache");
const iterator_1 = require("../iterator");
const interface_1 = require("../pb/interface");
const list_1 = require("../pb/list");
const util_1 = require("../util");
const HEADER_PAGE = 0;
const INITIAL_ROOT_PAGE = 1;
const MIN_NODE_LENGTH = cache_1.PAGE_SIZE >> 1;
const filename = (name) => path.join(args_1.dataDir, `${name}.list`);
async function checkIsList(name) {
    const collections = await _1.getCollections;
    const collection = collections[name];
    if (collection !== interface_1.CollectionType.LIST) {
        throw new Error(`Collection ${name} is not a list`);
    }
}
const getHeader = (name) => new cache_1.FilePage(filename(name), HEADER_PAGE).use(async (page) => list_1.headerType.toObject(list_1.headerType.decodeDelimited(new Uint8Array(page)), { longs: Number }));
const setHeader = (name, header) => new cache_1.FilePage(filename(name), HEADER_PAGE).use(async (page) => new Uint8Array(page).set(list_1.headerType.encodeDelimited(header).finish()));
const getNode = (name, page) => new cache_1.FilePage(filename(name), page).use(async (page) => list_1.nodeType.toObject(list_1.nodeType.decodeDelimited(new Uint8Array(page)), { defaults: true, longs: Number }));
const setNode = (name, page, node) => new cache_1.FilePage(filename(name), page).use(async (page) => new Uint8Array(page).set(list_1.nodeType.encodeDelimited(node).finish()));
// Obtains a free page from the free list, or by extending the file.
// Not atomic, so only one call should be performed at a time.
async function getFreePage(name, header) {
    const file = filename(name);
    const freePage = header.freePage.next;
    if (freePage === list_1.FREE_LIST_END) {
        const pages = await cache_1.getPageCount(file);
        await cache_1.setPageCount(file, pages + 1);
        return pages;
    }
    else {
        header.freePage = await new cache_1.FilePage(file, freePage).use(async (page) => list_1.freePageType.toObject(list_1.freePageType.decodeDelimited(new Uint8Array(page))));
        return freePage;
    }
}
// Atomically adds a free page to the front of the free list
const addFreePage = (name, header, pageNo) => new cache_1.FilePage(filename(name), pageNo).use(async (page) => {
    const { freePage } = header;
    new Uint8Array(page).set(list_1.freePageType.encodeDelimited(freePage).finish());
    freePage.next = pageNo;
});
// Performs the tree lookup, traversing the path to the leaf
async function lookup(name, { child: { size, page } }, index, insert = false) {
    if (index === undefined)
        index = size; // inserting at the end of the list
    else {
        if (index < -size || index >= size + Number(insert)) {
            throw new Error(`Index ${index} is out of bounds in list of size ${size}`);
        }
        if (index < 0)
            index += size;
    }
    const path = [];
    let lookupPage = page;
    while (true) {
        const node = await getNode(name, lookupPage);
        const pathItem = { page: lookupPage, index, node };
        path.push(pathItem);
        if ('leaf' in node)
            return path; // at the end of the path
        // Find the child containing the search index
        let nodeIndex = 0;
        for (const { size, page } of node.inner.children) {
            if (index < size || insert && index === size) {
                lookupPage = page;
                break;
            }
            index -= size;
            nodeIndex++;
        }
        pathItem.index = nodeIndex;
    }
}
// Removes and returns the right half of an array
const split = (arr) => arr.splice(arr.length >> 1);
// Appends another array to the left or right of an array
function concat(arr, other, left) {
    if (left)
        arr.unshift(...other);
    else
        arr.push(...other);
}
// Returns the length of the sublist under a node
const nodeSize = (node) => 'inner' in node
    ? node.inner.children.reduce((totalSize, { size }) => totalSize + size, 0)
    : node.leaf.values.length;
// Extracts information about the parent of the current node
// from its path (with the current node popped off)
function getParent(path) {
    const [parent] = path.slice(-1);
    if (!parent)
        return undefined;
    const { node: parentNode, index } = parent;
    // istanbul ignore if
    if ('leaf' in parentNode)
        throw new Error('Parent is not an inner node?');
    const { children } = parentNode.inner;
    return { children, index };
}
// Saves a node and any of its ancestors that need saving,
// handling any overflows that occur
async function saveWithOverflow(name, path, header, insert) {
    let madeSplit = false;
    while (path.length) {
        const { page, node } = path.pop();
        const parent = getParent(path);
        try {
            await setNode(name, page, node);
            // Saved node without overflowing
            if (insert) {
                if (parent) {
                    const { children, index } = parent;
                    children[index].size++;
                }
            }
            else
                break; // ancestor sizes don't need updating, so we're done
        }
        catch (e) {
            // Node overflowed
            util_1.ensureOverflowError(e);
            // TODO: this doesn't split leaves evenly
            const newNode = 'leaf' in node
                // Make copies of values since they are slices of the old page,
                // which will be overwritten
                ? { leaf: { values: split(node.leaf.values).map(value => value.slice()) } }
                : { inner: { children: split(node.inner.children) } };
            const newPage = await getFreePage(name, header);
            const children = [
                { size: nodeSize(node), page },
                { size: nodeSize(newNode), page: newPage }
            ];
            const promises = [
                setNode(name, page, node),
                setNode(name, newPage, newNode)
            ];
            if (parent) { // replace the 1 child in the parent with 2
                parent.children.splice(parent.index, 1, ...children);
            }
            else { // splitting the root node
                promises.push((async () => {
                    const rootPage = await getFreePage(name, header);
                    header.child.page = rootPage;
                    await setNode(name, rootPage, { inner: { children } });
                })());
            }
            await Promise.all(promises);
            madeSplit = true;
        }
    }
    if (insert)
        header.child.size++;
    // Only write header if necessary
    if (insert || madeSplit)
        await setHeader(name, header);
}
// Tries to coalesce a node with its siblings; returns whether successful
async function tryCoalesce(name, node, path, header) {
    const parent = getParent(path);
    if (!parent)
        return false; // root node can't be coalesced
    const { len } = list_1.nodeType.encodeDelimited(node);
    if (len >= MIN_NODE_LENGTH)
        return false; // ensure node is sufficiently empty
    // Find possible siblings to coalesce with
    const { children, index } = parent;
    const thisChild = children[index];
    const file = filename(name);
    const siblingLengths = await Promise.all([true, false]
        .map(left => {
        const siblingIndex = left ? index - 1 : index + 1;
        return { left, siblingIndex, sibling: children[siblingIndex] };
    })
        .filter(({ sibling }) => sibling) // skip siblings that don't exist
        .map(async (sibling) => {
        const length = await util_1.getNodeLength(file, sibling.sibling.page);
        return { sibling, length };
    }));
    // Coalescing is only possible with less than half-full siblings
    const coalesceCandidates = siblingLengths.filter(({ length }) => length < MIN_NODE_LENGTH);
    if (!coalesceCandidates.length)
        return false;
    const newFreePages = [];
    while (true) {
        // Choose the smaller of the candidate siblings to coalesce
        const coalesceSibling = util_1.argmin(coalesceCandidates, ({ length }) => length);
        // Coalesce with selected sibling
        const [{ sibling }] = coalesceCandidates.splice(coalesceSibling, 1);
        const { left, siblingIndex, sibling: { page: siblingPage } } = sibling;
        const siblingNode = await getNode(name, siblingPage);
        if ('leaf' in node) { // coalescing leaf nodes
            // istanbul ignore if
            if ('inner' in siblingNode)
                throw new Error('Invalid sibling?');
            // Copy sibling's values because they are slices of its page,
            // which will be overwritten when it gets added to the free list
            concat(node.leaf.values, siblingNode.leaf.values.map(value => value.slice()), left);
        }
        else { // coalescing inner nodes
            // istanbul ignore if
            if ('leaf' in siblingNode)
                throw new Error('Invalid sibling?');
            concat(node.inner.children, siblingNode.inner.children, left);
        }
        newFreePages.push(siblingPage);
        // Remove sibling from parent
        children.splice(siblingIndex, 1);
        // See if it is possible to coalesce with the other sibling
        const coalesceAgain = coalesceCandidates.length &&
            list_1.nodeType.encodeDelimited(node).len < MIN_NODE_LENGTH;
        if (coalesceAgain) {
            // The index of the right sibling goes down if the left sibling is removed
            if (left)
                coalesceCandidates[0].sibling.siblingIndex--;
        }
        else
            break;
    }
    // Update sublist's size
    thisChild.size = nodeSize(node);
    // Make this the new root if it is the only child of the root node
    if (path.length === 1 && children.length === 1) {
        path.pop();
        newFreePages.push(header.child.page);
        header.child = thisChild;
    }
    const promises = newFreePages.map(page => addFreePage(name, header, page));
    promises.push(setNode(name, thisChild.page, node));
    await Promise.all(promises);
    return true;
}
// Recursively generates the entries of a sublist within a given index range
async function* sublistEntries(name, page, start, end) {
    const node = await getNode(name, page);
    if ('inner' in node) {
        for (const { page, size } of node.inner.children) {
            if (end <= 0)
                break;
            yield* sublistEntries(name, page, start, end);
            start -= size;
            end -= size;
        }
    }
    else {
        const { values } = node.leaf;
        start = Math.max(start, 0);
        end = Math.min(end, values.length);
        for (let i = start; i < end; i++)
            yield values[i];
    }
}
async function* listEntries(name, start, end) {
    const { child: { page, size } } = await getHeader(name);
    yield* sublistEntries(name, page, start || 0, end === undefined ? size : end);
}
const iterators = new iterator_1.Iterators();
async function create(name) {
    await _1.addCollection(name, interface_1.CollectionType.LIST);
    const file = filename(name);
    await cache_1.createFile(file);
    await cache_1.setPageCount(file, 2); // allocate the header page and root node
    await Promise.all([
        setHeader(name, {
            child: { size: 0, page: INITIAL_ROOT_PAGE },
            freePage: { next: list_1.FREE_LIST_END }
        }),
        setNode(name, INITIAL_ROOT_PAGE, { leaf: { values: [] } })
    ]);
}
exports.create = create;
async function drop(name) {
    await checkIsList(name);
    iterators.checkNoIterators(name);
    await Promise.all([_1.dropCollection(name), cache_1.removeFile(filename(name))]);
}
exports.drop = drop;
// "delete" is a reserved name, so we use "remove" instead
async function remove(name, listIndex) {
    await checkIsList(name);
    iterators.checkNoIterators(name);
    const header = await getHeader(name);
    const path = await lookup(name, header, listIndex);
    const [{ index, node }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    node.leaf.values.splice(index, 1);
    // Save leaf and all its ancestors
    let coalesced = true;
    while (path.length) {
        const { page, node } = path.pop();
        // Only coalesce if child was coalesced
        coalesced = coalesced && await tryCoalesce(name, node, path, header);
        if (!coalesced) { // did not coalesce, so save node
            await setNode(name, page, node);
            const parent = getParent(path);
            const parentChild = parent
                ? parent.children[parent.index]
                : header.child;
            parentChild.size--;
        }
    }
    await setHeader(name, header);
}
exports.remove = remove;
async function get(name, listIndex) {
    await checkIsList(name);
    const header = await getHeader(name);
    const path = await lookup(name, header, listIndex);
    const [{ index, node }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    return node.leaf.values[index];
}
exports.get = get;
async function insert(name, listIndex, value) {
    await checkIsList(name);
    iterators.checkNoIterators(name);
    const header = await getHeader(name);
    const path = await lookup(name, header, listIndex, true);
    const [{ index, node }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    node.leaf.values.splice(index, 0, value);
    await saveWithOverflow(name, path, header, true);
}
exports.insert = insert;
async function set(name, listIndex, value) {
    await checkIsList(name);
    iterators.checkNoIterators(name);
    const header = await getHeader(name);
    const path = await lookup(name, header, listIndex);
    const [{ index, node }] = path.slice(-1);
    // istanbul ignore if
    if ('inner' in node)
        throw new Error('Path does not end in a leaf?');
    node.leaf.values[index] = value;
    await saveWithOverflow(name, path, header, false);
}
exports.set = set;
async function size(name) {
    await checkIsList(name);
    const { child } = await getHeader(name);
    return child.size;
}
exports.size = size;
async function iter(name, start, end) {
    await checkIsList(name);
    return iterators.registerIterator(name, listEntries(name, start, end));
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
